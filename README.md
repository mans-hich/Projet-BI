# Projet BI  
# Projet de Business Intelligence Northwind

## Présentation du projet
Ce projet propose une **solution complète de Business Intelligence** basée sur le jeu de données Northwind.  
Il couvre l’ensemble du cycle de vie BI : extraction des données à partir de sources hétérogènes, transformation à l’aide de Python, chargement dans un entrepôt de données SQL Server, et visualisation via un tableau de bord interactif Power BI.

---

## Objectifs
- Intégrer des données provenant de plusieurs sources (SQL Server, Microsoft Access, Excel)
- Concevoir et implémenter un entrepôt de données basé sur un schéma en étoile
- Mettre en place un pipeline ETL automatisé en Python
- Calculer les indicateurs clés de performance (KPI)
- Développer un tableau de bord interactif avec Power BI
- Soutenir la prise de décision basée sur les données

---

## Architecture globale

Sources de données  
(SQL Server / Excel)  
↓  
Processus ETL (Python : pandas, pyodbc)  
↓  
Entrepôt de données (SQL Server – NEWW)  
↓  
Tableau de bord Power BI


---

## Structure du Projet

Northwind-BI-Project/
│
├── data/
│ ├── Customers.xlsx
│ ├── Employees.xlsx
│ ├── Orders.xlsx
│ └── etc
│
├── scripts/
│ ├── DatabaseConfig.py
│ ├── etl.py
│ ├── create_database.py
│ ├── etl_main.py
│ ├── validation_exploration.py
│
├── notebooks/
│ └── exploration.ipynb
│
├── figures/
│ └── dashboard_preview.png
│
├── reports/
│ ├── BI_Project_Report.docx
│ └── BI_Project_Report.pdf
│
├── video/
│ └── demo_dashboard.mp4
│
├── README.md
└── .gitignore

