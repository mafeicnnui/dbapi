#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/6 9:17
# @Author  : ma.fei
# @File    : urls.py.py
# @Software: PyCharm

from urls.backup   import backup
from urls.syncer   import syncer
from urls.datax    import datax
from urls.transfer import transfer
from urls.archive  import archive
from urls.monitor  import monitor
from urls.instance import instance
from urls.slowlog  import slowlog
from urls.health   import health
from urls.alert    import alert
from urls.bbgl     import bbgl

urls = []
urls.extend(backup)
urls.extend(syncer)
urls.extend(datax)
urls.extend(transfer)
urls.extend(archive)
urls.extend(monitor)
urls.extend(instance)
urls.extend(slowlog)
urls.extend(health)
urls.extend(alert)
urls.extend(bbgl)
