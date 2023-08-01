FILE = document

build: 
	pdflatex -interaction=nonstopmode $(FILE).tex
	rm -f $(FILE).aux $(FILE).log $(FILE).fdb_latexmk $(FILE).synctex.gz $(FILE).fls

clean: 
	rm -f $(FILE).aux $(FILE).log $(FILE).fdb_latexmk $(FILE).synctex.gz $(FILE).fls $(FILE).pdf
