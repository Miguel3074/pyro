import sys
import threading
import time
import Pyro5.api
from datetime import datetime


CLIENTE_ID = sys.argv[1]
NOME_OBJETO_PYRO = f"cliente.exclusao_mutua.{CLIENTE_ID}"

TEMPO_MAXIMO_SC = 30  # segundos


@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
class ClienteHandle(object):
    def __init__(self):
        self.status = "RELEASED"
        self.timestamp_pedido = None
        self.fila_pedidos = []
        self.respostas_recebidas = set()
        self.timer_recurso = None
        self.start_time_sc = None

    def receber_pedido(self, requisitante_id, timestamp_req):
        print(f"[Thread Pyro] Recebeu pedido de {requisitante_id} com timestamp {timestamp_req}")
        
        if self.status == "HELD" or (self.status == "WANTED" and self.timestamp_pedido < timestamp_req):
            print(f"[Thread Pyro] Status é {self.status}. Colocando {requisitante_id} na fila.")
            self.fila_pedidos.append(requisitante_id)
            return "WAIT"
        else:
            print(f"[Thread Pyro] Status é {self.status}. Respondendo 'OK' para {requisitante_id}.")
            return "OK"
        
    def liberar_sc(self, automatico=False):
        origem = "automaticamente (tempo expirou)" if automatico else "manualmente"
        print(f"\nCliente {CLIENTE_ID} saindo da SC {origem}...")
        
        self.status = "RELEASED"
        self.timestamp_pedido = None
        self.start_time_sc = None

        if self.fila_pedidos:
            print(f"Notificando {len(self.fila_pedidos)} peers na fila de espera...")
            ns = Pyro5.api.locate_ns()
            
            for requisitante_id in self.fila_pedidos:
                nome_peer = f"cliente.exclusao_mutua.{requisitante_id}"
                try:
                    uri_peer = ns.lookup(nome_peer)
                    proxy_peer = Pyro5.api.Proxy(uri_peer)
                    proxy_peer.receber_ok(CLIENTE_ID)
                    
                    print(f"Notificou {requisitante_id} com 'OK'")
                except Exception as e:
                    print(f"Erro ao notificar {requisitante_id}: {e}")

            self.fila_pedidos.clear()

    def receber_ok(self, remetente_id):
        print(f"[Thread Pyro] Recebeu 'OK' do peer {remetente_id}.")
        self.respostas_recebidas.add(remetente_id)
        return "ACK"


def iniciar_servidor_pyro(handler):
    daemon = Pyro5.api.Daemon()
    ns = Pyro5.api.locate_ns()
    uri = daemon.register(handler)
    ns.register(NOME_OBJETO_PYRO, uri)
    
    daemon.requestLoop()


def interface_usuario(handler):
    time.sleep(1) 
    while True:
        if handler.status == "WANTED":
            ns = Pyro5.api.locate_ns()
            peers = ns.list(prefix="cliente.exclusao_mutua.")
            peers.pop(NOME_OBJETO_PYRO, None)
            
            if len(handler.respostas_recebidas) >= len(peers):
                print("\n[AUTO] Recebeu 'OK' de todos os peers. Entrando na SC!")
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

        ns = Pyro5.api.locate_ns()
        peers = ns.list(prefix="cliente.exclusao_mutua.")
        peers.pop(NOME_OBJETO_PYRO, None)

        if opcao == '1':
            if handler.status != "RELEASED":
                print("Ação inválida: O cliente já está na fila ou na SC.")
                continue

            print(f"Cliente {CLIENTE_ID} quer entrar na SC...")
            handler.status = "WANTED"
            handler.timestamp_pedido = datetime.now().timestamp()
            handler.respostas_recebidas.clear()
            
            if not peers:
                print("Nenhum outro peer encontrado. Entrando na SC diretamente.")
                handler.status = "HELD"
                continue

            print(f"Enviando pedido para {len(peers)} peers...")
            for nome_peer, uri_peer in peers.items():
                try:
                    proxy_peer = Pyro5.api.Proxy(uri_peer)
                    resposta = proxy_peer.receber_pedido(CLIENTE_ID, handler.timestamp_pedido)
                    if resposta == "OK":
                        handler.respostas_recebidas.add(nome_peer)
                        print(f"Recebeu 'OK' de {nome_peer}")
                except Exception as e:
                    print(f"Erro ao contatar {nome_peer}: {e}")
            
            while len(handler.respostas_recebidas) < len(peers):
                print(f"Aguardando respostas... ({len(handler.respostas_recebidas)}/{len(peers)})")
                time.sleep(2)

            print("Recebeu 'OK' de todos os peers. Entrando na SC!")
            handler.status = "HELD"

            if handler.status == "HELD":
                print(f"Recurso obtido. O acesso expira em {TEMPO_MAXIMO_SC} segundos.")
                handler.timer_recurso = threading.Timer(TEMPO_MAXIMO_SC, handler.liberar_sc, args=[True])
                handler.start_time_sc = time.time()
                handler.timer_recurso.start()

        elif opcao == '2':
            if handler.status != "HELD":
                print("Ação inválida: O cliente não está na SC.")
                continue
            
            if handler.timer_recurso and handler.timer_recurso.is_alive():
                print("Cancelando timer de liberação automática.")
                handler.timer_recurso.cancel()
            
            handler.liberar_sc(automatico=False)


        elif opcao == '3':
            print("Peers ativos no sistema:")
            try:
                ns = Pyro5.api.locate_ns()
                peers = ns.list(prefix="cliente.exclusao_mutua.")
                for nome, uri in peers.items():
                    if nome != NOME_OBJETO_PYRO:
                        print(f"  - {nome.split('.')[-1]}")
            except Pyro5.errors.NamingError:
                print("Erro: Não foi possível localizar o Name Server.")

        elif opcao == '4':
            print("Saindo do sistema...")
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
