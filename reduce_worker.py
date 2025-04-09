import socket
import pickle
from collections import Counter
from threading import Thread

stored_counts = Counter()

def handle_connection(conn, addr,HOST,port):
    """Fonction du thread
       Ecoute les données et agit en conséquences
       Si ce que l'on recoit n'est pas NO_MORE_DATA alors on l'ajoute a notre comptage
       SI on recoit NO_MORE_DATA alors on envoie nos données de comptage"""
    global stored_counts
    try:
        data = b''#création d'un buffer qui recoit les packets
        while True:
            packet = conn.recv(4096)
            if not packet: #si il n'y a plus de packets on a fini pour ce reducer
                break
            data += packet

        if data == b'NO_MORE_DATA': #il n'y a plus de données arrivant d'un mapper
            print(f"Reducer {port} a terminé sa réception de données.")
            envoie_donnes_coordinator(port,HOST)

        else:#les données recues viennent du mapper
            partial_counts = pickle.loads(data)
            stored_counts.update(partial_counts) #mets ajour les données avec les données entrantes utilise la libraire collections
            
    except Exception as e:
        print(f"Erreur lors de la réception depuis {addr}: {e}")
    finally:
        conn.close()

def envoie_donnes_coordinator(port,HOST):
    """Envoi des résultats au coordinateur"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:#connection au socket port 8000
        s.connect((HOST, 8000))
        s.sendall(pickle.dumps(dict(stored_counts)))#envoie des données
        print(f"Reducer {port} a renvoyé ses données")


def run_reduce_worker(host, port):
    """Fonction principale du reducer
       Ecoute les données que l'ont recoit sur son port
       Démarre un thread pour le receveur qui fait tourner la fonction handle_connection*
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen() #ecoute les données entrantes
        print(f"Reduce worker actif sur {host}:{port}")
        while True:
            conn, addr = s.accept()
            t = Thread(target=handle_connection, args=(conn, addr,host,port))
            t.start() #démarre le thread
