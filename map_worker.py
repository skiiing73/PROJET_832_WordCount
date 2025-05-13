import hashlib
import socket
import pickle
from collections import Counter
import re

def run_map_worker(host, port, filepath, nb_reducers, reduce_port_base,map_done_port):
    """Fonction principale du mapper elle gère les appels aux sous fonctions"""
    print(f"Maper worker actif sur {host}:{port}")
    word_counts=lecture_fichier(filepath)

    envoi_data_recever(word_counts,nb_reducers,host,reduce_port_base,port)
    
    notifier_coordinator(host,map_done_port)


def lecture_fichier(filepath):
    """Lecture du fichier et comptage des mots """

    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read().lower() 
        words = re.findall(r'\b\w+\b', text) #suppresion espaces
        word_counts = Counter(words)
    return word_counts


def double_hash(word, nb_reducers):
    """Double hachage pour améliorer la répartition des données"""

    #utiliser SHA-256 et MD5 pour obtenir un hachage unique pour chaque mot
    hash1 = hashlib.sha256(word.encode('utf-8')).hexdigest()
    hash2 = hashlib.md5(word.encode('utf-8')).hexdigest()

    hash_combined = int(hash1, 16) ^ int(hash2, 16) #combinaison des deux hachages

    return hash_combined % nb_reducers


def envoi_data_recever(word_counts, nb_reducers, host, reduce_port_base, port):
    """Choisi comment répartir les données entre les receveurs
       Envoie les données aux receveurs
    """
    reduce_data = {i: {} for i in range(nb_reducers)}
    #repartition des mots par double hachage
    for word, count in word_counts.items():
        reduce_id = double_hash(word, nb_reducers)
        reduce_data[reduce_id][word] = count

    #envoi aux reduce workers 
    for reduce_id, data in reduce_data.items():
        if not data:
            continue
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, reduce_port_base + reduce_id))
                s.sendall(pickle.dumps(data))
                print(f"Le mapper {port} a envoyé ses données au reducer {reduce_port_base+reduce_id}")
        except ConnectionRefusedError:
            print(f"Erreur de connexion au reduce worker {reduce_id}")


def notifier_coordinator(host,map_done_port):
    """Signaler au coordinateur que le travail est fini"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((host, map_done_port))
            s.sendall(b"MAPPER_DONE")
        except Exception as e:
            print(f"Erreur en envoyant MAPPER_DONE: {e}")