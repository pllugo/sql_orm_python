#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Pedro Luis Lugo Garcia"
__email__ = "pllugo@gmail.com"
__version__ = "1.1"


import os
import csv
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///secundaria.db")
base = declarative_base()
session = sessionmaker(bind=engine)()

from config import config

# Obtener la path de ejecución actual del script
script_path = os.path.dirname(os.path.realpath(__file__))

# Obtener los parámetros del archivo de configuración
config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)

class Autor(base):
    __tablename__ = "autor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Autor: {self.name}"


class Libro(base):
    __tablename__ = "libro"
    id = Column(Integer, primary_key=True)
    titulo = Column(String)
    cantidad_paginas = Column(Integer)
    autor_id = Column(Integer, ForeignKey("autor.id"))

    autor = relationship("Autor")

    def __repr__(self):
        return f"Libro: {self.titulo}, cantidad_paginas {self.cantidad_paginas}, autor {self.autor.name}"


def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)


def insert_autor(name):
    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Crear una nueva nacionalidad
    autor = Autor(name=name)

    # Agregar la nacionalidad a la DB
    session.add(autor)
    session.commit()

def insert_libro(titulo, cantidad_paginas, autor):
     # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Buscar el autor del libro
    query = session.query(Autor).filter(Autor.name == autor)
    autor = query.first()

    if autor is None:
        # Podrá ver en este ejemplo que sucederá este error con la persona
        # de nacionalidad Inglaterra ya que no está definida en el archivo
        # de nacinoalidades
        print(f"Error no existe el Autor {autor}")
        return

    # Crear la persona
    book = Libro(titulo=titulo, cantidad_paginas=cantidad_paginas, autor=autor)
    book.autor = autor

    # Agregar la persona a la DB
    session.add(book)
    session.commit()

def fill():
    print('Completemos esta tablita!')
    # Insertar el archivo CSV de nacionalidades
    # Insertar fila a fila
    with open(dataset['autores']) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            insert_autor(row['autor'])

    # Insertar el archivo CSV de personas
    # Insertar todas las filas juntas
    with open(dataset['libros']) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            insert_libro(row['titulo'], int(row['cantidad_paginas']), row['autor_id'])

def fetch(id=0):
    print('Comprovemos su contenido, ¿qué hay en la tabla?')
    # Crear una query para imprimir en pantalla
    # todos los objetos creaods de la tabla estudiante.
    # Imprimir en pantalla cada objeto que traiga la query
    # Realizar un bucle para imprimir de una fila a la vez

    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Buscar todas las personas
    query = session.query(Libro).order_by(Libro.cantidad_paginas.desc())

    # Si está definido el limite aplicarlo
    if id > 0:
        query = query.limit(id)

    # Leer una persona a la vez e imprimir en pantalla
    for libros in query:
        print(libros)

def search_author(titulo):
    print('Operación búsqueda!')
    # Esta función recibe como parámetro el nombre de un posible tutor.
    # Crear una query para imprimir en pantalla
    # aquellos estudiantes que tengan asignado dicho tutor.

    # Para poder realizar esta query debe usar join, ya que
    # deberá crear la query para la tabla estudiante pero
    # buscar por la propiedad de tutor.name

    # Crear la session
    Session = sessionmaker(bind=engine)
    session = Session()

     # Buscar el tutor en la base de datos
    resultado = session.query(Libro).join(Libro.autor).filter(Libro.titulo == titulo)

    for dato in resultado:
        author = dato.autor
    
    
    if author is None:
        # Podrá ver en este ejemplo que sucederá este error con la persona
        # de nacionalidad Inglaterra ya que no está definida en el archivo
        # de nacinoalidades
        print(f"Error en este libro {titulo}")
        return
    
    return author

if __name__ == "__main__":
  # Crear DB
  create_schema()

  # Completar la DB con el CSV
  fill()

  # Leer filas
  fetch()  # Ver todo el contenido de la DB
  fetch(3)  # Ver la fila 3
  #fetch(20)  # Ver la fila 20

  # Buscar autor
  print(search_author('Relato de un naufrago'))

