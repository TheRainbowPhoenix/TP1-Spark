# Bienvenue dans mon cours

Ce projet a pour but de fournir les bases d'un projet PySpark, 
et quelques exercices à compléter progressivement.

## Structure du projet PySpark
```
├── README.md
├── src
│   ├── main.py
│   ├── exercises
│   │   └── exo_1_wordcount
│   │       └── __init__.py
│   │   └── exo_2_wordcount
│   │       └── __init__.py
...
```

Le fichier `README.md` contient en général
- la documentation du projet,
- les étapes d'installation
- comment utiliser le projet
- comment maintenir et contribuer au projet

Le fichier `src/main.py` est le point d'entrée du job.
On l'invoque avec les arguments en commande en ligne et exécute dynamiquement le job demandé.

## Tester un bout de code
1. Se positionner dans le répertoire `src/tests`
2. Lancer le package python `unittest` en ligne de commande ou par IDE
```bash
python -m unittest test_1_wordcount.TestParallelize
```
3. Amusez-vous à créer d'autres cas de tests, des mocks
4. Essayez avec une autre librairie comme `pytest`

## Fonctionnalités couvertes par ces exercices
- RDD
- Transformations et Actions
- Group By
- Order By
- Window Functions
- Interaction BDD NoSQL MongoDB