FROM python:3.12-slim

WORKDIR /app

COPY code/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY code/main.py code/database.py code/debate.py ./
COPY code/static/ static/

RUN mkdir -p data

EXPOSE 8000

CMD ["python", "main.py"]
