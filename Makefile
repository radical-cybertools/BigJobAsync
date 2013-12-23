
.PHONY: clean

clean:
	-rm -rf temp/ build/ *.egg-info/ temp/ MANIFEST dist/ bigjobasync/VERSION pylint.out *.egg VERSION
	find . -name \*.pyc -exec rm -f {} \;

