
# Use the official Python image
FROM python:3.12-slim


# Update package lists
RUN apt-get update && apt-get install -y wget apt-transport-https curl

# Install dependencies and Google Chrome
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl unzip ca-certificates \
    libx11-dev libxkbfile-dev libgdk-pixbuf2.0-0 libnss3 \
    libasound2 libatk-bridge2.0-0 libgtk-3-0 libxss1 \
    fonts-liberation libappindicator3-1 libu2f-udev lsb-release \
    && curl -sSL https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb -o google-chrome.deb \
    && dpkg -i google-chrome.deb || apt-get -y -f install \
    && rm google-chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip selenium webdriver-manager

# Set working directory
WORKDIR /app

# Copy requirements and application files
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/

# Environment variables for Chrome
ENV PATH="/usr/bin/google-chrome:$PATH"
ENV DISPLAY=:99

# Run the application
CMD ["python", "main.py"]