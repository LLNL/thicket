---
thebe-kernel: python3
---
# Thicket Tutorial


Structures of thicket:

```{thebe-button}
```

```{code-block} python
:class: thebe

import os
import thicket as tt
import pandas as pd

lassen1 = [f"data/lassen/XL_BaseCuda_01048576_0{x}.cali" for x in range(1, 4)]
lassen2 = [f"data/lassen/XL_BaseCuda_04194304_01.cali"]

th = tt.Thicket.from_caliperreader(lassen1 + lassen2)

pd.set_option('display.max_columns', 500)
th.metadata


```
