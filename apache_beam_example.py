import apache_beam as beam

def split_file(element):
	numero, titlulo, ranked = element.split(',')
	dicc = {'numero': numero,
			'titulo': str(titlulo),
			'ranked': float(ranked)
		}
	return [dicc]

def filter_movie(element):
	return element['ranked'] > 4


p1 = beam.Pipeline()
movie_gt_4 = (
	p1
#Leer archivo, saltar encabezado
| beam.io.ReadFromText('.\movies_beam\movie_gt_4.txt', skip_header_lines=1)
#uso de la funcion ParDo, elemento de transformacion usando una funcion.
| beam.ParDo(split_file)
#uso del filtro definido en la linea 11.
| 'filtro' >> beam.Filter(filter_movie)
#escritura del filtrado de datos en un archivo, o imprimiendolos en consola, debido al bajo registro
#| beam.io.WriteToText('result.txt')
| "printeo" >> beam.Map(print)
)
p1.run()
