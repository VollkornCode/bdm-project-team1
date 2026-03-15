# Big Data Management Spring 2026 - Project

This repository serves as store for the code infrastrucutre of the BDM Project.


## Setup
### Environment Variables
In the `config` folder, Copy the `.env.template` file and name it to `.env`, then fill in the values. Ask a maintainer of the project to provide you with the values for the secrets.

### Docker
Change the working directory to `src/docker` and run `docker compose up --build -d`

### Running locally
If you are testing your code locally, in `src/config/conf.py` set `MINIO_LOCAL = True` such that you can run the code from your local machine and don't need to run it via docker.

To run your python script locally, change the working directory to `src` and run `python main.py`:
```sh
$ user@machine bdm-project-team1/src % python main.py
```