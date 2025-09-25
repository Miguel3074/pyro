import sys
import threading
import time
import Pyro5.api
from datetime import datetime

if len(sys.argv) < 2:
    print("Uso: python cliente_tolerante_falhas.py <SEU_ID>")
    sys.exit(1)

CLIENTE_ID = sys.argv[1]
NOME_OBJETO_PYRO = f"cliente.exclusao_mutua.{CLIENTE_ID}"

TEMPO_MAXIMO_SC = 30
HEARTBEAT_INTERVALO = 15
HEARTBEAT_TIMEOUT = 45
REQUEST_TIMEOUT = 10 

@Pyro5.api.behavior(instance_mode="single")
class ClienteHandle(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.status = "RELEASED"
        self.timestamp_pedido = None
        self.fila_pedidos = []
        self.respostas_recebidas = set()
        self.timer_recurso = None
        self.peers_ativos = set()
        self.ultimo_heartbeat = {}
        self.pedidos_pendentes = {}

    @Pyro5.api.expose
    def receber_pedido(self, requisitante_id, timestamp_req):
        with self.lock:
            print(f"[Thread Pyro] Recebeu pedido de {requisitante_id} com timestamp {timestamp_req}")
            
            if requisitante_id not in self.peers_ativos:
                print(f"[Thread Pyro] Ignorando pedido de peer desconhecido ou inativo: {requisitante_id}")
                return "IGNORED"

            if self.status == "HELD" or (self.status == "WANTED" and (self.timestamp_pedido < timestamp_req or (self.timestamp_pedido == timestamp_req and CLIENTE_ID < requisitante_id))):
                print(f"[Thread Pyro] Status é {self.status}. Colocando {requisitante_id} na fila.")
                self.fila_pedidos.append(requisitante_id)
                return "WAIT"
            else:
                print(f"[Thread Pyro] Status é {self.status}. Respondendo 'OK' para {requisitante_id}.")
                return "OK"

    @Pyro5.api.expose
    def receber_ok(self, remetente_id):
        entrar_agora = False
        with self.lock:
            if self.status == "WANTED":
                print(f"[Thread Pyro] Recebeu 'OK' do peer {remetente_id}.")
                self.respostas_recebidas.add(remetente_id)
                if remetente_id in self.pedidos_pendentes:
                    del self.pedidos_pendentes[remetente_id]
                
                if self.respostas_recebidas.issuperset(self.peers_ativos):
                    self.status = "HELD"
                    entrar_agora = True
        
        if entrar_agora:
            self.executar_entrada_sc()

        return "ACK"

    @Pyro5.api.expose
    def receber_heartbeat(self, remetente_id):
        with self.lock:
            print(f"[Heartbeat] recebido de: {remetente_id}")
            if remetente_id not in self.peers_ativos:
                print(f"[Heartbeat] Peer novo ou reconectado detectado: {remetente_id}")
                self.peers_ativos.add(remetente_id)
            self.ultimo_heartbeat[remetente_id] = time.time()
        return "ACK"

    def liberar_sc(self, automatico=False):
        with self.lock:
            if self.status != "HELD":
                return

            origem = "automaticamente (tempo expirou)" if automatico else "manualmente"
            print(f"\nCliente {CLIENTE_ID} saindo da SC {origem}...")
            
            self.status = "RELEASED"
            self.timestamp_pedido = None
            self.pedidos_pendentes.clear()

            if not self.fila_pedidos:
                return

            print(f"Notificando {len(self.fila_pedidos)} peers na fila de espera...")
            ns = Pyro5.api.locate_ns()
            peers_na_fila = self.fila_pedidos[:]
            self.fila_pedidos.clear()

        for requisitante_id in peers_na_fila:
            if requisitante_id in self.peers_ativos:
                try:
                    uri_peer = ns.lookup(f"cliente.exclusao_mutua.{requisitante_id}")
                    proxy_peer = Pyro5.api.Proxy(uri_peer)
                    proxy_peer.receber_ok(CLIENTE_ID)
                    print(f"Notificou {requisitante_id} com 'OK'")
                except Exception as e:
                    print(f"Erro ao notificar {requisitante_id} (pode ter falhado): {e}")

    def executar_entrada_sc(self):
        print("\n\n[AUTO] Acesso à SC concedido!")
        print(f"Recurso obtido. O acesso expira em {TEMPO_MAXIMO_SC} segundos.")
        print(">>> Pressione Enter para atualizar o menu. <<<")
        
        with self.lock:
            if self.timer_recurso and self.timer_recurso.is_alive():
                self.timer_recurso.cancel()
            self.timer_recurso = threading.Timer(TEMPO_MAXIMO_SC, self.liberar_sc, args=[True])
            self.timer_recurso.start()

def iniciar_servidor_pyro(handler):
    daemon = Pyro5.api.Daemon()
    ns = Pyro5.api.locate_ns()
    uri = daemon.register(handler)
    ns.register(NOME_OBJETO_PYRO, uri)
    print(f"[Thread Pyro] Servidor iniciado para {NOME_OBJETO_PYRO}.")
    daemon.requestLoop()

def enviar_heartbeats(handler):
    ns = Pyro5.api.locate_ns()
    while True:
        time.sleep(HEARTBEAT_INTERVALO)
        
        with handler.lock:
            peers_atuais = list(handler.peers_ativos)

        if not peers_atuais:
            continue
        
        for peer_id in peers_atuais:
            try:
                uri_peer = ns.lookup(f"cliente.exclusao_mutua.{peer_id}")
                proxy_peer = Pyro5.api.Proxy(uri_peer)
                proxy_peer.receber_heartbeat(CLIENTE_ID)
            except Exception:
                pass

def verificar_falhas(handler):
    while True:
        time.sleep(HEARTBEAT_TIMEOUT / 2)
        
        peers_a_remover = set()
        agora = time.time()
        entrar_agora = False

        with handler.lock:
            for peer_id, ultimo_hb in handler.ultimo_heartbeat.items():
                if agora - ultimo_hb > HEARTBEAT_TIMEOUT:
                    peers_a_remover.add(peer_id)
            
            if peers_a_remover:
                print(f"\n[Detector de Falhas] Peers inativos detectados: {list(peers_a_remover)}")
                handler.peers_ativos.difference_update(peers_a_remover)
                for peer_id in peers_a_remover:
                    if peer_id in handler.ultimo_heartbeat:
                        del handler.ultimo_heartbeat[peer_id]
                    if handler.status == "WANTED":
                        handler.respostas_recebidas.add(peer_id)
                
                if handler.status == "WANTED" and handler.respostas_recebidas.issuperset(handler.peers_ativos):
                    handler.status = "HELD"
                    entrar_agora = True
        
        if entrar_agora:
            handler.executar_entrada_sc()

def verificar_timeouts_de_pedido(handler):
    while True:
        time.sleep(REQUEST_TIMEOUT / 2)

        pedidos_expirados = set()
        entrar_agora = False
        agora = time.time()

        with handler.lock:
            if handler.status != "WANTED":
                continue

            for peer_id, timestamp_envio in list(handler.pedidos_pendentes.items()):
                if agora - timestamp_envio > REQUEST_TIMEOUT:
                    pedidos_expirados.add(peer_id)

            if pedidos_expirados:
                print(f"\n[Request Timeout] Não houve resposta de {list(pedidos_expirados)} a tempo.")
                for peer_id in pedidos_expirados:
                    if peer_id in handler.pedidos_pendentes:
                        del handler.pedidos_pendentes[peer_id]
                    handler.respostas_recebidas.add(peer_id)

                if handler.respostas_recebidas.issuperset(handler.peers_ativos):
                    handler.status = "HELD"
                    entrar_agora = True

        if entrar_agora:
            handler.executar_entrada_sc()

def interface_usuario(handler):
    time.sleep(2)
    ns = Pyro5.api.locate_ns()
    
    try:
        todos_os_peers = ns.list(prefix="cliente.exclusao_mutua.")
        peer_ids = {nome.split('.')[-1] for nome in todos_os_peers if nome != NOME_OBJETO_PYRO}
        with handler.lock:
            handler.peers_ativos.update(peer_ids)
            for peer_id in peer_ids:
                handler.ultimo_heartbeat[peer_id] = time.time()
        print(f"Peers iniciais detectados: {list(peer_ids)}")
    except Exception as e:
        print(f"Erro ao buscar peers iniciais: {e}")

    while True:
        print("\n" + "="*50)
        print(f" CLIENTE: {CLIENTE_ID} | STATUS: {handler.status}")
        print(f" Peers Ativos Conhecidos: {sorted(list(handler.peers_ativos)) if handler.peers_ativos else 'Nenhum'}")
        print("="*50)
        print("Opções:")
        print("  1 - Pedir para entrar na Seção Crítica (SC)")
        print("  2 - Liberar a Seção Crítica (SC)")
        print("  3 - Sair")
        print("="*50)

        try:
            opcao = input("Digite uma opção: ").strip()
        except (EOFError, KeyboardInterrupt):
            opcao = '3'

        if opcao == '1':
            if handler.status != "RELEASED":
                print("Ação inválida: O cliente já está na fila ou na SC.")
                continue

            with handler.lock:
                print(f"\nCliente {CLIENTE_ID} quer entrar na SC...")
                handler.status = "WANTED"
                handler.timestamp_pedido = datetime.now().timestamp()
                handler.respostas_recebidas.clear()
                handler.pedidos_pendentes.clear()
                peers_para_pedir = list(handler.peers_ativos)

            if not peers_para_pedir:
                print("Nenhum outro peer ativo encontrado. Entrando na SC diretamente.")
                with handler.lock:
                    handler.status = "HELD"
                handler.executar_entrada_sc()
                continue

            print(f"Enviando pedido para {len(peers_para_pedir)} peers: {peers_para_pedir}...")
            for peer_id in peers_para_pedir:
                with handler.lock:
                    handler.pedidos_pendentes[peer_id] = time.time()
                
                try:
                    uri_peer = ns.lookup(f"cliente.exclusao_mutua.{peer_id}")
                    proxy_peer = Pyro5.api.Proxy(uri_peer)
                    resposta = proxy_peer.receber_pedido(CLIENTE_ID, handler.timestamp_pedido)
                    
                    if resposta == "OK":
                        handler.receber_ok(peer_id)
                    elif resposta == "WAIT":
                        print(f"Peer {peer_id} respondeu com 'WAIT'. Aguardando liberação.")
                        with handler.lock:
                            if peer_id in handler.pedidos_pendentes:
                                del handler.pedidos_pendentes[peer_id]
                                
                except Exception as e:
                    print(f"Erro ao contatar {peer_id}: {e}")
            
            print("Aguardando respostas...")

        elif opcao == '2':
            if handler.status != "HELD":
                print("Ação inválida: O cliente não está na SC.")
                continue
            
            if handler.timer_recurso and handler.timer_recurso.is_alive():
                handler.timer_recurso.cancel()
            
            handler.liberar_sc(automatico=False)

        elif opcao == '3':
            print("\nSaindo do sistema...")
            if handler.timer_recurso and handler.timer_recurso.is_alive():
                handler.timer_recurso.cancel()
            try:
                ns.remove(NOME_OBJETO_PYRO)
            except Exception as e:
                print(f"Não foi possível desregistrar do Name Server: {e}")
            sys.exit(0)
        
        elif opcao:
            print("Opção inválida!")
            
if __name__ == "__main__":
    try:
        Pyro5.api.locate_ns()
    except Pyro5.errors.NamingError:
        print("Iniciando um Name Server local...")
        threading.Thread(target=Pyro5.nameserver.start_ns_loop, daemon=True).start()
        time.sleep(1)

    cliente_handler = ClienteHandle()

    threading.Thread(target=iniciar_servidor_pyro, args=(cliente_handler,), daemon=True).start()
    threading.Thread(target=enviar_heartbeats, args=(cliente_handler,), daemon=True).start()
    threading.Thread(target=verificar_falhas, args=(cliente_handler,), daemon=True).start()
    threading.Thread(target=verificar_timeouts_de_pedido, args=(cliente_handler,), daemon=True).start()
    
    interface_usuario(cliente_handler)
