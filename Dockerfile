FROM python:3
RUN mkdir /app
WORKDIR /app
ADD parser/parser.py /app/
RUN pip install -r requirements.txt

CMD["python", "/app/parser.py"]
