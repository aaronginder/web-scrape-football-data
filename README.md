# Web Scrape Premier League Football Data

![Premier League Header](resources/article/premier-league-header.png "Premier League")

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#insallation)
3. [Usage](#usage)
4. [Data Sources](#data-sources)
5. [Contributions](#contributions)
6. [License](#license)
7. [Contact](#contact)

## Introduction

Generates a CSV of all games in the Premier League reported on `https://www.football-data.co.uk` via web scraping in Python. Repo contains an orchestrated pipeline to generate and process data from the website.

## Insallation

Below are the pre-requisites that can be used to generate the expected output of this repository.

|Software   | Version   |
|-----------|-----------|
|Python     | `^3.12`   |

Follow the instructions below to install the package dependencies by executing the commands:

- `pip install poetry` - installs the package dependency manager
- `poetry install`  - installs the Python package dependencies

## Usage

1. Run the web scraper module

     ```bash
     python src/main.py
     ```

2. Navigate to the `src/target/` folder for the processed outputs

## Data Sources

- [Football Data](https://www.football-data.co.uk/)

## Contributions

We welcome contributions to this project. Please follow these steps:

- Fork the repository.
- Create a new branch (git checkout -b feature/your-feature).
- Commit your changes (git commit -am 'Add some feature').
- Push to the branch (git push origin feature/your-feature).
- Create a new Pull Request.

## License

This project is licensed under the GNU General Public License. See the [LICENSE](LICENSE) file for details.

## Contact

- LinkedIn: [Aaron Ginder](https://www.linkedin.com/in/aaron-ginder/)
- Email: <aaronginder@hotmail.co.uk>

<a href="https://www.linkedin.com/in/aaron-ginder/">  <img src="resources/article/linkedin-icon.png" alt="LinkedIn" width="10%"></a>
