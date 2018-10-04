FROM jupyter/minimal-notebook

# -- python dependencies
COPY ./requirements.txt requirements.txt
RUN pip install -U -r requirements.txt
