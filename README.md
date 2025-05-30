# Brazilian Vehicle Fleet Data Pipeline

This project automates the process of downloading, cleaning, transforming, and storing data on the Brazilian municipal vehicle fleet, as provided by the National Traffic Secretariat (Senatran). The data is extracted from government web pages, transformed into a normalized format, and loaded into a PostgreSQL database for further analysis and visualization.

## Project Purpose

The goal is to create a robust and automated ETL pipeline that ensures timely updates of official vehicle fleet data. The data can then be explored and visualized in business intelligence tools such as Tableau or Power BI.

## Technologies Used

- Python
- pandas
- requests
- BeautifulSoup
- SQLAlchemy
- PostgreSQL

## Pipeline Summary

1. Access official pages from Senatran containing fleet data for the years 2024 and 2025.
2. Locate and download Excel files that contain municipal-level vehicle registration information.
3. Normalize column names and extract relevant metadata such as the reference month and year.
4. Reshape the data to long format with standardized columns: state (UF), municipality, date, vehicle type, and quantity.
5. Store the final dataset into a PostgreSQL table.

## Database Table

Table name: `frota_municipio_tipo_veiculo`

| Column       | Type    | Description                      |
|--------------|---------|----------------------------------|
| UF           | text    | Federative unit (state code)     |
| MUNICIPIO    | text    | Municipality name                |
| DATA         | date    | Reference date (month/year)      |
| TIPO_VEICULO | text    | Type of vehicle                  |
| QUANTIDADE   | integer | Number of vehicles registered    |

## Folder Structure
project/
├── main.py
├── requirements.txt
├── README.md
└── data/
├── raw/
├── trusted/
└── refined/


## How to Run

1. Clone the repository
2. Install the required packages with `pip install -r requirements.txt`
3. Configure PostgreSQL connection parameters in `main.py`
4. Run the script using `python main.py`

## Data Modeling

The project follows a multi-layered architecture:

- **Raw**: Original Excel files as downloaded from the web
- **Trusted**: Cleaned and structured data with consistent schema
- **Refined**: Final transformed dataset used for dashboarding

## License

This project is intended for educational and analytical purposes. No affiliation with Senatran or official data providers.
