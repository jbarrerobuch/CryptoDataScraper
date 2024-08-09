# CryptoDataScraper
=====================

Market Data Extractor for Financial Instruments on Deribit
--------------------------------------------------------

## Overview
------------

CryptoDataScraper is a Python-based application designed to collect market data of financial instruments from Deribit, a popular cryptocurrency derivatives exchange. The app utilizes APIs from Deribit and Binance to gather data on various instruments, including options and indexes.

## Features
------------

* Collects market data on financial instruments from Deribit
* Supports multiple instruments, including options and indexes
* Utilizes APIs from Deribit and Binance for data collection
* Stores collected data in a PostgreSQL database
* Provides a flexible and customizable framework for data analysis and processing

## Requirements
---------------

* Python 3.7+
* Deribit API credentials (apiKey, apisecret)
* Binance API credentials (apiKey, apisecret)
* PostgreSQL database setup (db_name, db_user, db_password)
* Required libraries: `lib.Agent`, `variables`, `datetime`, `time`, [os](cci:4://d:/OneDrive/Cosas PY/CryptoDataScraper/CryptoDataScraper/LICENSE:57:0-91:0), `memory_profiler`

## Usage
---------

1. Clone the repository and navigate to the project directory.
2. Install required libraries using `pip install -r requirements.txt`.
3. Set up your Deribit and Binance API credentials in the `variables.py` file.
4. Configure your PostgreSQL database settings in the `variables.py` file.
5. Run the application using `python main.py`.

## Configuration
----------------

The application can be customized by modifying the `variables.py` file. This file contains settings for:

* Deribit API credentials
* Binance API credentials
* PostgreSQL database settings
* Instrument list (options and indexes)

## Troubleshooting
------------------

* Check the application logs for errors and exceptions.
* Verify API credentials and database settings.
* Consult the Deribit and Binance API documentation for troubleshooting guides.

## Contributing
---------------

Contributions are welcome! If you'd like to contribute to the project, please fork the repository and submit a pull request with your changes.

## License
----------

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments
------------------

* Deribit API documentation: <https://www.deribit.com/api/>
* Binance API documentation: <https://binance-docs.github.io/apidocs/spot/en/>
* PostgreSQL documentation: <https://www.postgresql.org/docs/>