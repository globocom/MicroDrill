.PHONY: setup clean test upload
SPARK_TAR=tests/spark.tar.gz
SPARK_DIR=tests/spark
SPARK_OLD_DIR=spark-1.6.0-bin-hadoop2.4
SPARK_URL=http://ftp.unicamp.br/pub/apache/spark/spark-1.6.0/$(SPARK_OLD_DIR).tgz

setup:
	@pip install -r test_requirements.txt
	@rm -rf $(SPARK_DIR)
	@wget -nv -t 100 -T 15 -c "$(SPARK_URL)" -O $(SPARK_TAR)
	@tar xzf $(SPARK_TAR)
	@rm -rf $(SPARK_TAR)
	@mv $(SPARK_OLD_DIR) $(SPARK_DIR)

clean:
	@find . -iname '*.pyc' -delete
	@rm -rf *.egg-info dist

test: clean
	@nosetests -sd tests/ --exclude tests/spark --with-coverage --cover-package=microdrill

upload: clean
	@python setup.py -q sdist upload -r pypi
