import sys
import threading
import time
import Pyro5.api  # type: ignore
from datetime import datetime


CLIENTE_ID = sys.argv[1]
NOME_OBJETO_PYRO = f"cliente.exclusao_mutua.{CLIENTE_ID}"

TEMPO_MAXIMO_SC = 30
INTERVALO_HEARTBEAT = 15
TIMEOUT_HEARTBEAT = 30
TIMEOUT_RESPOSTA = 35


@Pyro5.api.behavior(instance_mode="single")
class ClienteHandle(object):
    def __init__(self):
        self.status = "RELEASED"
        self.timestamp_pedido = None
        self.fila_pedidos = []
        self.respostas_recebidas = set()
        self.timer_recurso = None
        self.start_time_sc = None
        
        self.ultimo_heartbeat = {}
        self.heartbeat_thread = None
        self.heartbeat_running = True
        
        self.pedidos_pendentes = {}
        self.lock = threading.Lock()
        
        self.iniciar_heartbeat()

    def iniciar_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target=self.enviar_heartbeats)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        verificador_thread = threading.Thread(target=self.verificar_heartbeats)
        verificador_thread.daemon = True
        verificador_thread.start()

    def enviar_heartbeats(self):
        while self.heartbeat_running:
            try:
                ns = Pyro5.api.locate_ns()
                peers = ns.list(prefix="cliente.exclusao_mutua.")
                peers.pop(NOME_OBJETO_PYRO, None)
                
                for nome_peer, uri_peer in peers.items():
                    try:
                        proxy_peer = Pyro5.api.Proxy(uri_peer)
                        proxy_peer.receber_heartbeat(CLIENTE_ID)
                    except Exception as e:
                        print(f" Erro ao enviar heartbeat para {nome_peer.split('.')[-1]}: {e}")
                        
            except Exception as e:
                print(f" Erro ao localizar peers para heartbeat: {e}")
            
            time.sleep(INTERVALO_HEARTBEAT)

    def verificar_heartbeats(self):
        while self.heartbeat_running:
            tempo_atual = time.time()
            peers_inativos = []
            
            with self.lock:
                for peer_id, ultimo_tempo in list(self.ultimo_heartbeat.items()):
                    if tempo_atual - ultimo_tempo > TIMEOUT_HEARTBEAT:
                        peers_inativos.append(peer_id)
                        del self.ultimo_heartbeat[peer_id]
                        
                for peer_id in peers_inativos:
                    if peer_id in self.fila_pedidos:
                        self.fila_pedidos.remove(peer_id)
                    
                    if peer_id in self.pedidos_pendentes:
                        del self.pedidos_pendentes[peer_id]
            
            if peers_inativos:
                print(f"Peers considerados inativos: {peers_inativos}")
                
            time.sleep(INTERVALO_HEARTBEAT)

    @Pyro5.api.expose
    def receber_heartbeat(self, peer_id):
        print(f"Recebeu heartbeat de {peer_id}")
        with self.lock:
            self.ultimo_heartbeat[peer_id] = time.time()
        return "ACK"

    def peer_esta_ativo(self, peer_id):
        with self.lock:
            if peer_id not in self.ultimo_heartbeat:
                return False
            tempo_atual = time.time()
            return (tempo_atual - self.ultimo_heartbeat[peer_id]) <= TIMEOUT_HEARTBEAT

    @Pyro5.api.expose
    def receber_pedido(self, requisitante_id, timestamp_req):

        if self.status == "HELD" or (self.status == "WANTED" and self.timestamp_pedido < timestamp_req):
            self.fila_pedidos.append(requisitante_id)
            return "DENIED"
        else:
            return "OK"
        
    def liberar_sc(self, automatico=False):
        origem = "automaticamente (tempo expirou)" if automatico else "manualmente"
        
        self.status = "RELEASED"
        self.timestamp_pedido = None
        self.start_time_sc = None
        self.respostas_recebidas = set()
        self.pedidos_pendentes = {}

        if self.fila_pedidos:
            ns = Pyro5.api.locate_ns()
            
            fila_ativa = [peer_id for peer_id in self.fila_pedidos if self.peer_esta_ativo(peer_id)]
            
            for requisitante_id in fila_ativa:
                nome_peer = f"cliente.exclusao_mutua.{requisitante_id}"
                try:
                    print(f"Notificando {requisitante_id}...")
                    uri_peer = ns.lookup(nome_peer)
                    proxy_peer = Pyro5.api.Proxy(uri_peer)
                    proxy_peer.receber_ok(CLIENTE_ID)
                    
                    print(f"Notificou {requisitante_id} com 'OK'")
                except Exception as e:
                    print(f"Erro ao notificar {requisitante_id}: {e}")

            self.fila_pedidos.clear()

    @Pyro5.api.expose
    def receber_ok(self, remetente_id):
        with self.lock:
            self.respostas_recebidas.add(remetente_id)
            if remetente_id in self.pedidos_pendentes:
                del self.pedidos_pendentes[remetente_id]
        return "ACK"

    def verificar_timeouts_respostas(self, peers_esperados):
        tempo_atual = time.time()
        peers_timeout = []
        
        with self.lock:
            for peer_id in list(self.pedidos_pendentes.keys()):
                if tempo_atual - self.pedidos_pendentes[peer_id] > TIMEOUT_RESPOSTA:
                    if self.peer_esta_ativo(peer_id):
                        print(f"Timeout de Resposta: Peer {peer_id} está ativo, mas ocupado. Estendendo a espera.")
                        self.pedidos_pendentes[peer_id] = time.time() # Reseta o timer para este peer
                    else:
                        print(f"Timeout: Peer {peer_id} não respondeu E seu heartbeat falhou. Removendo.")
                        peers_timeout.append(peer_id)
                        del self.pedidos_pendentes[peer_id]

        return peers_timeout

    def parar_heartbeat(self):
        print("Parando threads de heartbeat...")
        self.heartbeat_running = False


def iniciar_servidor_pyro(handler):
    daemon = Pyro5.api.Daemon()
    ns = Pyro5.api.locate_ns()
    uri = daemon.register(handler)
    ns.register(NOME_OBJETO_PYRO, uri)
    daemon.requestLoop()


def interface_usuario(handler):
    time.sleep(5)
    while True:
        if handler.status == "WANTED":
            ns = Pyro5.api.locate_ns()
            peers = ns.list(prefix="cliente.exclusao_mutua.")
            peers.pop(NOME_OBJETO_PYRO, None)
            
            peers_ativos = {nome: uri for nome, uri in peers.items() 
                            if handler.peer_esta_ativo(nome.split('.')[-1])}
            
            if len(handler.respostas_recebidas) >= len(peers_ativos):
                handler.status = "HELD"
                
                print(f"Recurso obtido. O acesso expira em {TEMPO_MAXIMO_SC} segundos.")
                handler.timer_recurso = threading.Timer(TEMPO_MAXIMO_SC, handler.liberar_sc, args=[True])
                handler.start_time_sc = time.time()
                handler.timer_recurso.start()

        print("\n" + "="*40)
        print(f" CLIENTE: {CLIENTE_ID} | STATUS: {handler.status}")
        print("="*40)
        print("Opções:")
        print("  1 - Pedir para entrar na Seção Crítica (SC)")
        print("  2 - Liberar a Seção Crítica (SC)")
        print("  3 - Ver peers ativos")
        print("  4 - Sair")
        print("="*40)

        opcao = input("Digite uma opção: ").strip()

        try:
            ns = Pyro5.api.locate_ns()
            peers = ns.list(prefix="cliente.exclusao_mutua.")
            peers.pop(NOME_OBJETO_PYRO, None)
        except Exception as e:
            print(f"Não foi possível contatar o Name Server: {e}")
            continue

        if opcao == '1':
            if handler.status != "RELEASED":
                continue
            handler.status = "WANTED"
            handler.timestamp_pedido = datetime.now().timestamp()
            handler.respostas_recebidas.clear()
            
            peers_ativos = {nome: uri for nome, uri in peers.items() 
                            if handler.peer_esta_ativo(nome.split('.')[-1])}
            
            if not peers_ativos:
                handler.status = "HELD"
                
                print(f"Recurso obtido. O acesso expira em {TEMPO_MAXIMO_SC} segundos.")
                handler.timer_recurso = threading.Timer(TEMPO_MAXIMO_SC, handler.liberar_sc, args=[True])
                handler.start_time_sc = time.time()
                handler.timer_recurso.start()
                continue

            print(f"Enviando pedido para {len(peers_ativos)} peers ativos: {list(p.split('.')[-1] for p in peers_ativos.keys())}")
            
            with handler.lock:
                for nome_peer in peers_ativos.keys():
                    peer_id = nome_peer.split('.')[-1]
                    handler.pedidos_pendentes[peer_id] = time.time()
            
            for nome_peer, uri_peer in peers_ativos.items():
                peer_id = nome_peer.split('.')[-1]
                try:
                    print(f"Enviando pedido para {peer_id}...")
                    proxy_peer = Pyro5.api.Proxy(uri_peer)
                    resposta = proxy_peer.receber_pedido(CLIENTE_ID, handler.timestamp_pedido)
                    if resposta == "OK":
                        handler.respostas_recebidas.add(peer_id)
                        with handler.lock:
                            if peer_id in handler.pedidos_pendentes:
                                del handler.pedidos_pendentes[peer_id]

                    elif resposta == "DENIED":
                        with handler.lock:
                            if peer_id in handler.pedidos_pendentes:
                                handler.pedidos_pendentes[peer_id] = time.time() # Reseta o timer

                except Exception as e:
                    with handler.lock:
                        if peer_id in handler.pedidos_pendentes:
                            del handler.pedidos_pendentes[peer_id]
            
            tempo_inicio_espera = time.time()
            #timeout
            while len(handler.respostas_recebidas) < len(peers_ativos):
                peers_timeout = handler.verificar_timeouts_respostas(list(peers_ativos.keys()))
                
                for peer_timeout in peers_timeout:
                    nome_timeout = f"cliente.exclusao_mutua.{peer_timeout}"
                    if nome_timeout in peers_ativos:
                        del peers_ativos[nome_timeout]
                
                if len(handler.respostas_recebidas) >= len(peers_ativos):
                    break
                    
                print(f"Aguardando respostas... ({len(handler.respostas_recebidas)}/{len(peers_ativos)})")
                time.sleep(1)
                
                if time.time() - tempo_inicio_espera > TIMEOUT_RESPOSTA * (len(peers_ativos) + 1):
                    print("Timeout geral de espera atingido. Saindo do loop.")
                    break

            if handler.status == "WANTED": # Evita entrar na SC se já entrou pelo loop principal
                print("Recebeu respostas suficientes. Entrando na SC!")
                handler.status = "HELD"

                if handler.status == "HELD":
                    print(f"Recurso obtido. O acesso expira em {TEMPO_MAXIMO_SC} segundos.")
                    handler.timer_recurso = threading.Timer(TEMPO_MAXIMO_SC, handler.liberar_sc, args=[True])
                    handler.start_time_sc = time.time()
                    handler.timer_recurso.start()

        elif opcao == '2':
            if handler.status != "HELD":
                print("Ação inválida: cliente não está na SC.")
                continue
            
            if handler.timer_recurso and handler.timer_recurso.is_alive():
                print("Cancelando timer de liberação automática.")
                handler.timer_recurso.cancel()
            
            handler.liberar_sc(automatico=False)

        elif opcao == '3':
            try:
                ns = Pyro5.api.locate_ns()
                peers = ns.list(prefix="cliente.exclusao_mutua.")
                peers_encontrados = False
                for nome, uri in peers.items():
                    if nome != NOME_OBJETO_PYRO:
                        peer_id = nome.split('.')[-1]
                        status_heartbeat = "ATIVO" if handler.peer_esta_ativo(peer_id) else "INATIVO"
                        print(f"  - {peer_id} ({status_heartbeat})")
                        peers_encontrados = True
                
                if not peers_encontrados:
                    print("  Nenhum outro peer encontrado.")
            except Pyro5.errors.NamingError:
                print("Não foi possível localizar o Name Server.")

        elif opcao == '4':
            handler.parar_heartbeat()
            try:
                ns = Pyro5.api.locate_ns()
                ns.remove(NOME_OBJETO_PYRO)
            except Exception as e:
                print(f"Não foi possível desregistrar do Name Server: {e}")
            break
        
        else:
            print("Opção inválida!")


if __name__ == "__main__":
    cliente_handler = ClienteHandle()

    pyro_thread = threading.Thread(target=iniciar_servidor_pyro, args=(cliente_handler,))
    pyro_thread.daemon = True
    pyro_thread.start()
    interface_usuario(cliente_handler)