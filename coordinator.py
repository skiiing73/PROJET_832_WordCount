import os
import socket
import pickle
from threading import Thread
from multiprocessing import Process
import time

from matplotlib import pyplot as plt
from map_worker import run_map_worker
from reduce_worker import run_reduce_worker

HOST = 'localhost'
MAP_PORT_BASE = 5000
REDUCE_PORT_BASE = 6000
MAP_DONE_PORT = 7000
REDUCE_DONE_PORT = 8000


def start_map_workers(files,NB_REDUCERS):
    """ Démarre les processus mappers sur les ports 5001, 5002 etc..."""
    workers = []
    for i, file in enumerate(files):
        port = MAP_PORT_BASE + i
        p = Process(target=run_map_worker, args=(HOST, port, file, NB_REDUCERS, REDUCE_PORT_BASE,MAP_DONE_PORT))
        p.start()
        workers.append(p)
    return workers

def start_reduce_workers(NB_REDUCERS):
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

def notify_reducers_done(NB_REDUCERS):
    """ Quand les mappers ont fini coordinator previent les reducers qu'ils ne recevront plus rien"""
    for i in range(NB_REDUCERS):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, REDUCE_PORT_BASE + i))
            s.sendall(b'NO_MORE_DATA')

def waiting_reducers_done(NB_REDUCERS):
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
    files = ["fichiers_test/exemple_court.txt", "fichiers_test/exemple_long.txt", "fichiers_test/exemple_test.txt","fichiers_test/exemple_tres_long.txt"]
    times = []

    for NB_REDUCERS in range(1, 6):
        print(f"\n==== Test avec {NB_REDUCERS} reducers ====")
        start = time.time()

        reduce_processes = start_reduce_workers(NB_REDUCERS)
        map_processes = start_map_workers(files, NB_REDUCERS)

        done_thread = Thread(target=wait_for_all_mappers, args=(len(map_processes),))
        done_thread.start()

        for p in map_processes:
            p.join()
        done_thread.join()

        notify_reducers_done(NB_REDUCERS)
        print("Mappage terminé")

        result = waiting_reducers_done(NB_REDUCERS)

        with open(f'output/final_result_{NB_REDUCERS}_reducers.txt', 'w', encoding='utf-8') as f:
            for word, count in result.items():
                f.write(f"{word} : {count}\n")

        print("Reduction terminée\nRésultats disponibles")

        for p in reduce_processes:
            p.terminate()
            p.join()

        end = time.time()
        duration = end - start
        times.append(duration)
        print(f"Temps total avec {NB_REDUCERS} reducers : {duration:.2f} secondes")
        
    plt.plot(range(1, 6), times, marker='o')
    plt.xlabel('Nombre de reducers')
    plt.ylabel('Temps d\'exécution (s)')
    plt.title('Temps d\'exécution en fonction du nombre de reducers')
    plt.grid(True)
    plt.show()
