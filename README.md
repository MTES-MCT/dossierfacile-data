# dossierfacile-data

[![License](https://img.shields.io/badge/licence-MIT-blue.svg)](LICENSE)
[![DBT](https://img.shields.io/badge/dbt-v1.9.0-orange)](https://www.getdbt.com/)
[![Startup d'État](https://img.shields.io/badge/Startup%20d'État-DossierFacile-green)](https://dossierfacile.logement.gouv.fr)

Pipeline de transformation de données pour [DossierFacile](https://dossierfacile.fr).

## Table des matières

- [Présentation](#présentation)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Utilisation](#utilisation)
- [Tests](#tests)
- [Développement](#développement)
- [Optimisation](#optimisation)
- [Support](#support)
- [Licence](#licence)

## Présentation

Ce projet fait partie de l'écosystème DossierFacile et gère la transformation des données via DBT. Il permet de :

- Transformer et modéliser les données en provenance de notre application en production
- Assurer la qualité des données via la mise en place d'une modélisation et des tests
- Générer des analyses directement consommables par Metabase

## Architecture

Le projet suit une architecture DBT standard avec les composants suivants :

- `models/` : Modèles de données principaux
- `macros/` : Fonctions réutilisables
- `seeds/` : Données de référence
- `tests/` : Tests de données

### Structure des modèles

Les modèles sont organisés en 3 couches techniques :

- **Staging** : Première couche de transformation des données brutes
  - tenant (locataire)
  - owner (propriétaire) 
  - operation (opérations sur les dossiers)
  - document

- **Core** : Modèles métier enrichis et normalisés
  - tenant
  - owner
  - operation  
  - document

- **Analytics** : Modèles d'analyse et de reporting prêts à être utilisés par Metabase

## Installation

### Prérequis

- Python 3.10+
- PostgreSQL 15+
- DBT Core 1.9.0+
- Accès à la base de données DossierFacile

### Installation des dépendances

```bash
# Création de l'environnement virtuel
python -m venv .venv
source .venv/bin/activate

# Installation des dépendances
pip install dbt-core dbt-postgres
```

## Configuration

### Configuration de DBT

1. Créez un fichier `profiles.yml` à la racine du projet :

```yml
dossierfacile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {{ host }}
      user: {{ user}}
      password: {{ password }}
      port: {{ port }}
      dbname: {{ database }}
      schema: {{ schema }}
      threads: 3
```

2. Remplacez les valeurs `{{}}` avec celles du secret "user SQL df-data dbt_dev" dans vaultwarden.

3. Testez la connexion :
```bash
dbt debug
```

### Configuration de Sqlfluff

```bash
pip install sqlfluff sqlfluff-templater-dbt
```

## Utilisation

### Commandes principales

```bash
# Exécution complète
dbt run

# Exécution d'un modèle spécifique
dbt run -s model_name

# Tests unitaires
dbt test -s test_type:unit

# Génération de la documentation
dbt docs generate
dbt docs serve
```

### Linting SQL

```bash
# Vérification
sqlfluff lint

# Correction automatique
sqlfluff fix
```

## Tests

### Types de tests dans DBT

#### 1. Tests simples (Singular Tests)
Ces tests sont définis directement dans le fichier YAML du modèle et vérifient des contraintes basiques :

```yaml
version: 2

models:
  - name: core_tenant_account
    columns:
      - name: id
        data_tests:
          - unique
          - not_null:
              config:
                where: "id is not null"
```

- **unique** : Vérifie que chaque valeur de la colonne est unique
- **not_null** : Vérifie qu'aucune valeur n'est NULL
- **accepted_values** : Vérifie que les valeurs sont dans une liste prédéfinie
  
Il est également possible d'étendre ces capacités de tests avec des [tests génériques custom](https://docs.getdbt.com/best-practices/writing-custom-generic-tests)

#### 2. Tests unitaires (Unit Tests)

Tests plus complexes qui vérifient la logique de transformation des données. Exemple avec le test [core_tenant_account.test.yml](models/core/tenant/core_tenant_account.test.yml)

### Différences entre les types de tests

1. **Tests simples/generic**
   - Vérifient des contraintes basiques sur les colonnes
   - Faciles à mettre en place
   - Exemple : `unique`, `not_null`, `accepted_values`
   - Idéal pour la validation des données

2. **Tests unitaires**
   - Vérifient la logique de transformation complexe
   - Permettent de tester avec des données d'entrée contrôlées
   - Exemple : logique de détermination de l'origine du locataire
   - Idéal pour tester la logique métier

### Exécution des tests

```bash
# Tous les tests
dbt test

# Tests unitaires uniquement
dbt test -s test_type:unit

# Tests spécifiques à un modèle
dbt test -s core_tenant_account
```

## Développement

### Pré-commit hooks

1. Installation :
```bash
pip install pre-commit
```

2. Configuration dans `.pre-commit-config.yaml` :
```yaml
repos:
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.3.0
    hooks:
      - id: sqlfluff-lint
      - id: sqlfluff-fix
```

3. Activation :
```bash
pre-commit install
```

### Gestion des permissions PostgreSQL

```sql
-- Accès par défaut aux futures tables
alter default privileges 
for user sql_user 
in schema sql_schema 
grant select on tables 
to group sql_group;

-- Accès aux tables existantes
alter default privileges in schema sql_schema
grant select on tables to group sql_group;

-- Usage du schéma
grant usage on schema sql_schema to group sql_group;

-- Accès SELECT à toutes les tables
grant select on all tables in schema sql_schema to group sql_group;
```

## Optimisation

Pour palier des problèmes de time out sur des requêtes Metabase, il est possible d'indexer les tables au niveau de dbt pour accélérer le parcours de celle-ci rendant un grand nombre de requête plus rapide et donc visualisable sur Metabase.

Documentation offcielle dbt sur les index : [https://docs.getdbt.com/reference/resource-configs/materialize-configs?version=1.11#indexes]

Plusieurs types d'index sont disponibles avec dbt mais de manière générale, les gains de temps sont de l'ordre de O(n) -> O(log(n))
Par exemple, sur une table sans index de 20M de lignes, la recherche d'un id unique coûte 20M d'opérations en moyenne, avec index, on passe à environ 20 opérations.
De manière générale, plus le nombre de valeurs distinctes d'une colonne est grand, plus le gain de temps par indexation est important.

### Syntaxe 

```sql
{{ config(
    materialized = {'table','incremental'},  
    unique_key = '', -- sécurité qui permet d'éviter les doublons (quasi-obligatoire pour un type incremental)
    indexes=[ -- Liste des colonnes sur lesquelles on construit un index
      {'columns': {['id']}, 'unique': True}, 
      {'columns': ['tenant_id']},
      {'columns': ['guarantor_id']},
      {'columns': ['created_at'], 'type': 'brin'} --
    ]
) }}
```

### Paramètres supplémentaires

#### Materialized

table -> l'index est reconstruit à chaque fois que la table est exécutée | sûr mais très coûteux 

incremental -> l'index est reconstruit uniquement pour les "nouvelles valeurs" de la table | précautions nécessaires mais beaucoup plus performant

Pour indiquer les "nouvelles valeurs" d'une table incremental, i.e, les lignes à ajouter aux index; il est nécessaire d'inclure un filtre : 

```sql
{% if is_incremental() %}
        AND creation_date > (SELECT MAX(created_at) - INTERVAL '2 day' FROM {{ this }}) 
        -- Le fait de rajouter un INTERVAL est une sécurité pour éviter de ne pas indexer des lignes en cas d'erreur de réplication
    {% endif %}
````

Il est important de placer ce filtre au niveau de la lecture de la table source ou avant jointure de tables coûteuses.

*Attention* : Si placé après un GROUP BY ou une fonction de fenêtrage, il ne sert à rien car l'aggrégation aura déjà été faite sur toute la table

#### Colmuns

'unique': permet de s'assurer de l'unicité des valeurs de la colonne
'type' : `btree` (par défaut), `brin` (d'autres types existent, cf la doc dbt)

De manière générale, il faut privilégier les `btree` (balanced tree = recherche dichotomique).
Un index de type `brin` est utile lorsque l'agencement de la colonne est corollée à la logique d'écriture (ex: created_at : les 100 dernières dates les plus récentes correspondent aux 100 dernières lignes d'écriture). Il est beaucoup plus rapide et léger qu'un 'btree' mais ne sert que sur les colonnes avec une date ou un index parfaitement rangé (pas de UUID)

### Tests

Une fois les index mis en place, 

```bash
dbt run/build -s ma_table --full-refresh (pour le forcer à construire l'index sur toute la table)
```

Cela peut prendre plus de temps que d'habitude car la construction d'index est un processus assez lourd

Pour vérifier que les index ont bien été construits, on exécute la requête suivante sur DBeaver : 

```sql
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'ma_table';
```
Une fois qu'on est sûr que les index existent on peut faire les tests suivant pour avoir une idée des gains : 

```sql
EXPLAIN ANALYZE
SELECT * FROM ma_table WHERE my_column = my_value ;
```
=> On vérifie qu'on a bien un Index scan et pas un Seq scan et on regarde le Execution time

Ensuite, on relance la même requête en désactivant les index 

```sql
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;

EXPLAIN ANALYZE
SELECT * FROM ma_table WHERE my_column = my_value;
```
Si dans la fenêtre d'analyse on a un Seq scan c'est qu'ils sont bien désactivés. On peut comparer les Execution time.

Et on les remets avec : 

```sql
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;
```

## Support

Pour toute question ou problème :
- Contacter l'équipe technique de DossierFacile [contact@dossierfacile.fr](mailto:contact@dossierfacile.fr)

## Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.