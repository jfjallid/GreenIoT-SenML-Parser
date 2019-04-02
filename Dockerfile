FROM python:3
RUN mkdir /app
WORKDIR /app
ADD . /app/
RUN pip install attrs # dependency of python-senml. Pip does not install dependencies for github repos.
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "/app/parser/parser.py"]
