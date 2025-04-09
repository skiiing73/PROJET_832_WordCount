import os
import socket
import pickle
from threading import Thread
from multiprocessing import Process
from map_worker import run_map_worker
from reduce_worker import run_reduce_worker

HOST = 'localhost'
MAP_PORT_BASE = 5000
REDUCE_PORT_BASE = 6000
MAP_DONE_PORT = 7000
REDUCE_DONE_PORT = 8000
NB_REDUCERS = 3

def start_map_workers(files):
    """ Démarre les processus mappers sur les ports 5001, 5002 etc..."""
    workers = []
    for i, file in enumerate(files):
        port = MAP_PORT_BASE + i
        p = Process(target=run_map_worker, args=(HOST, port, file, NB_REDUCERS, REDUCE_PORT_BASE,MAP_DONE_PORT))
        p.start()
        workers.append(p)
    return workers

def start_reduce_workers():
    """ Démarre les processus reducers sur les ports 6001, 6002 etc..."""
    workers = []
    for i in range(NB_REDUCERS):
        port = REDUCE_PORT_BASE + i
        p = Process(target=run_reduce_worker, args=(HOST, port))
        p.start()
        workers.append(p)
    return workers

def wait_for_all_mappers(n_maps):
    """Attends que les mmapers ait fini leurs mappages et ai envoyé les données aux reducers.""" 
    done = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, MAP_DONE_PORT))
        s.listen()
        while done < n_maps:
            conn, addr = s.accept()
            with conn:
                data = conn.recv(1024)
                if data == b"MAPPER_DONE":
                    done += 1
                    print(f"Mapper terminé ({done}/{n_maps})")

def notify_reducers_done():
    """ Quand les mappers ont fini coordinator previent les reducers qu'ils ne recevront plus rien"""
    for i in range(NB_REDUCERS):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, REDUCE_PORT_BASE + i))
            s.sendall(b'NO_MORE_DATA')

def waiting_reducers_done():
    """Attends que les reducers ait fini leur travail
       La fonction est en attente sur un port et met a jour les resultats finaux a chaque fois qu'un reduce lui envoit
    """
    final_result = {}
    done = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, REDUCE_DONE_PORT))
        s.listen()
        while done < NB_REDUCERS:#attend que tous les reducers ai envoyé leur travail
            conn, addr = s.accept()
            with conn:
                data = b'' #création d'un buffer qui recoit les packets
                while True:
                    packet = conn.recv(4096)  
                    if not packet: #si il n'y a plus de packets on a fini pour ce reducer
                        break 
                    data += packet 
                try:
                    result = pickle.loads(data) #charge les résultats du buffer
                    final_result.update(result)
                    done += 1
                except Exception as e:
                    print(f"Erreur de désérialisation depuis {addr}: {e}")
    return final_result

if __name__ == '__main__':
    files = ["fichiers_test/exemple_court.txt","fichiers_test/exemple_long.txt","fichiers_test/exemple_test.txt"]

    reduce_processes = start_reduce_workers() #lance les reducers
    map_processes = start_map_workers(files) #lance les mappers

    n_maps=len(map_processes)
    done_thread = Thread(target=wait_for_all_mappers, args=(n_maps,)) #lance un thread permettant d'attendre la fin des mappers
    done_thread.start()

    for p in map_processes:
        p.join()

    done_thread.join()#thread est fini 

    notify_reducers_done() #notifie les receveurs
    print("Mappage terminé")
    result = waiting_reducers_done() #attends les resultats des receveurs

    with open('output/final_result.txt', 'w', encoding='utf-8') as f: #sauvergarde des données dans un txt
        for word, count in result.items():
            f.write(f"{word} : {count}\n")
    print("Reduction terminée\nRésulats disponibles")

    for p in reduce_processes:#éteint les processus reducers
        p.terminate()
        p.join()
