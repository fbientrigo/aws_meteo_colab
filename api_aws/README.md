# The API
The desing of the API its done to be as simple and direct as possible.
We are using Uvicorn, that can be easily run in a EC2 or local computer.

Al comenzar el server
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
```

El proyecto se puede clonar
```bash
mkdir aws_meteo_colab
git clone https://github.com/fbientrigo/aws_meteo_colab.git aws_meteo_colab
```

Luego partimos con el modelo
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```


# Datasets


- 251115 (crudo, real ECMWF)
    - steps: +24hrs (+6hrs, +1hrs)
        -  indices:
            - STI
            - SEPI
            