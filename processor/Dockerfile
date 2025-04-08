FROM python:3.8-slim

WORKDIR /app

# Copy and install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Copy project files into the container
COPY . /app
COPY . .

RUN chmod +x start_all.sh

CMD ["bash", "start_all.sh"]
