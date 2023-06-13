# Proyecto BD 2

### **Integrantes**

* Mario Rios Gamboa
* Luis Berrospi Rodriguez
* Angello Zuloaga Loo

## **Tabla de contenido**

- [**Proyecto BD 2**](#proyecto-bd-2)
    - [**Integrantes**](#integrantes)
    - [**Tabla de contenido**](#tabla-de-contenido)
- [**Introducción**](#introducción)
    - [**Objetivo**](#objetivo)
    - [**Dominio de Datos**](#dominio-de-datos)


# **Introducción**

## **Objetivo**

El siguiente proyecto consiste en la implementación de un motor de búsqueda a partir de una query textual para buscar documentos de un dataset. Para ello, se utilizarán algoritmos de búsqueda y recuperación de la información basado en el contenido para implementar de manera óptima el Índice Invertido. Finalmente se medirá el tiempo en el que se ejecuta y será comparado con el tiempo al ejecutarlo en Postgresql.

## **Dominio de Datos**

En cuanto a los datos, se trabaja con el dataset arXiv, el cual es un repositorio con 1.7 millones de artículos. Con fines de optimización, se decidió trabajar con un archivo reducido incluyendo los siguientes campos:

| **Campo** | **Tipo** |
| --- | --- |
| id | `string` |
| submitter | `string` |
| title | `string` |
| doi | `string` |
| abstract | `string` |
| update_date | `date` |

- Se utilizará el campo **abstract** para la aplicación del índice y realizar las consultas en base a este.
