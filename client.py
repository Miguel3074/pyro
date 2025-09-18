import sys
import Pyro5.api

CLIENTE_ID = sys.argv[1]

##################################################################################################################

status = "RELEASED"
@Pyro5.api.expose
class client_handle(object):
    def cliente_get_status():
        return status

##################################################################################################################

def interface_usuario():
    print("\n" + "="*60)
    print(" SISTEMA DE EXCLUSAO MUTUA - CLIENTE")
    print("="*60)
    print("Opções disponíveis:")
    print("  1 - Ver status do cliente")
    print("  2 - Pedir para entrar na SC")
    print("  3 - Sair da SC")
    print("  4 - Sair do sistema")
    print("="*60)
    saiu = False
    while True:
        try:
            opcao = input("\nDigite uma opção (1-4): ").strip()
            
            if opcao == '1':
                print(f"Status do cliente {CLIENTE_ID}: {status}")
                    
            elif opcao == '2':
                print(f"Cliente {CLIENTE_ID} pedindo para entrar na SC...")
                    
            elif opcao == '3':
                print(f"Cliente {CLIENTE_ID} saindo da SC...")
                    
            elif opcao == '4':
                print("Saindo do sistema...")
                saiu = True
                break                
            else:
                print("Opção inválida! Digite 1, 2, 3 ou 4.")
                
        except KeyboardInterrupt:
            print("\nSaindo do sistema...")
            break
        except Exception as e:
            print(f"Erro: {e}")

        if saiu:
            break

##################################################################################################################

daemon = Pyro5.server.Daemon()
ns = Pyro5.api.locate_ns()
uri = daemon.register(client_handle)
ns.register("SC.control", uri)

daemon.requestLoop()

name = input("What is your name? ").strip()
greeting_maker = Pyro5.api.Proxy("PYRONAME:SC.control")
print(greeting_maker.get_fortune(name))