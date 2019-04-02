FROM python:3
RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "/app/parser/parser.py"]
