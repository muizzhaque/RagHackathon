# This is to build docker image and deploy this image.

FROM python:3.11.10-slim

WORKDIR /RAGHACKATHON

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "example.py", "--server.port=8501", "--server.address=0.0.0.0"]
