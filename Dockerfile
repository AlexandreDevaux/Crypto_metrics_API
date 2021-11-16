FROM python

WORKDIR /program

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY src/ .

CMD ["python3", "./app.py"]
