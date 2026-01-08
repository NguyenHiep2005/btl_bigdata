--Cài môi trường:
# cài Java 11
    java -version
    sudo apt update
    sudo apt install openjdk-11-jdk
    java -version


# Cài Python 3.10
    sudo apt install python3.10 python3.10-venv python3.10-dev

    python3.10 --version


# Cài Docker
    sudo apt-get update
    sudo apt-get install docker.io docker-compose
    
    sudo usermod -aG docker $USER
    newgrp docker
    
    docker --version
    docker-compose --version


# Tạo Virtual Environment
    python3.10 -m venv venv
    
    source venv/bin/activate
    
    pip install --upgrade pip setuptools wheel
    
    pip install -r requirements.txt


# Tạo Terminal 1
    docker compose up

# Terminal 2



    python data_generator.py


# Terminal 3

    python prediction_pyspark.py


# Terminal 4

    python backend/app.py


# Truy cập giao diện web
http://localhost:5000
# Kiểm tra api
http://localhost:5000/api/current-predictions
# Xem database
http://localhost:8081
