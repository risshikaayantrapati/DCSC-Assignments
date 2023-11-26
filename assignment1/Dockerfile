# Docker file, Image, Container

FROM python:3.8 

RUN pip install pandas

WORKDIR /assignments
COPY example.py  example.py

#CMD ["python", "./example.py"]

ENTRYPOINT [ "python", "example.py" ]