# 🧠 WordCount MapReduce en Python

Ce projet implémente un système distribué simplifié de **comptage de mots** basé sur le modèle **MapReduce**, sans utiliser Hadoop. L'implémentation repose uniquement sur **Python**, avec l'utilisation de **processus** et de **sockets TCP** pour la communication.

## Fonctionnalités principales

- Prise en charge de plusieurs fichiers texte en entrée.
- **Phase Map** : chaque mapper (processus) lit un fichier, compte les occurrences de chaque mot.
- **Répartition par hash** : les mots sont répartis entre les reducers selon `hash(word) % nb_reducers`.
- **Phase Reduce** : chaque reducer cumule les comptes partiels des mots reçus.
- Un **coordinateur** central orchestre le processus :
  - Démarre les mappers et reducers
  - Surveille les mappers (relance si bloqués)
  - Notifie les reducers quand les mappers ont terminé
- Écriture du résultat final dans `output/final_result_<NB_REDUCERS>_reducers.txt`
- Affichage du **temps total d'exécution**
- **Statistiques** sur le temps en fonction du nombres de reducers.

## Axes d’amélioration futurs

- **Monitoring** et relance des reducers
- Répartition plus **fine** des mots (hashing avancé ou trie plus complexe)

### 1. Exécution
Placer dans le dossier `fichiers_test` tous les fichiers a analyser

Dans un terminal, lancer :

```bash
python coordinator.py
```

