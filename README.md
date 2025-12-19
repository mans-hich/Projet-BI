# Projet BI  
# Projet de Business Intelligence Northwind

## Présentation du projet
Ce projet propose une **solution complète de Business Intelligence** basée sur le jeu de données Northwind.  

Il couvre l’ensemble du cycle de vie BI : extraction des données à partir de sources hétérogènes, transformation à l’aide de Python, chargement dans un entrepôt de données SQL Server, et visualisation via un tableau de bord interactif Power BI.

---

## Structure du Projet

Northwind-BI-Project/

│

├── data/

| └──access/

|   ├──Nw.accdb

| └── excel/

│   ├── Customers.xlsx

│   ├── Employees.xlsx

│   ├── Orders.xlsx

│   └── etc

│

├── scripts/

│ ├── DatabaseConfig.py

│ ├── etl.py

│ ├── create_datawraehouse.py

│ ├── etl_main.py

│ ├── dashboard.py

│

├── notebooks/

│ └── notebook.ipynb

│

├── figures/

│ └── figures.png

│

├── reports/

│ └── Rapport_Projet_BI.pdf

│

├── video/

│ └── demo.mp4

│

├── README.md

└── .gitignore


### Installation
Clone the repository:
```bash
git clone https://github.com/mans-hich/Projet-BI.git
cd Projet-BI
```
Install dependencies:
```bash

pip install -r requirements.txt
```
Configure database connections in DatabaseConfig.py:

```bash

SQL_SERVER_CONFIG = {
    'server': 'localhost',
    'database': 'Northwind',
    'username': '..',
    'password': 'your_password'
}
```

Initialize the data warehouse:

```bash

python create_datawarehouse.py
```
Run the ETL pipeline:
```bash

python etl.py
```

Launch the dashboard:
```bash

streamlit run dashboard.py
```

## Credits

Hicham Manseur 

232337344206

ING3 CyberSecurity 

Groupe 03 


