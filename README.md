# Auntonomous transaction converter

## Motivation
The tools in the market are not available to translate the Oracle 

## Usage
### Create the container
Navigate to the directory of this Dockerfile and run  
```bash
docker build --no-cache -t autotran-converter .
```

### Run the container
Run the following command  
```bash
sudo docker run -it --rm --name autotran-converter -v /usr/local/google/home/josuegen/Documents/win/Export_20231106/Export:/usr/src/scripts/ autotran-converter
```