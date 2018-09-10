
```
text = spark.read.text('README.md')
text.count()
text.first()
lines_with_hadoop = text.filter(text.value.contains('hadoop'))

```

```
from pyspark.sql import functions as F

(text
    .select(F.size(F.split(text.value, '\s+')).name('num_words'))
    .agg(F.max(F.col('num_words')))
    .collect())

```

```
from pyspark.sql import functions as F


word_count = (
    text
    .select(F.explode(F.split(text.value, '\s+')).alias('word'))
    .groupBy('word')
    .count())
word_count.collect()

```

```
word_count.cache()

```