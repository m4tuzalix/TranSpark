# Transpark

**Composable and Cacheable Transformation Framework for PySpark**

[![PyPI version](https://badge.fury.io/py/transpark.svg)](https://pypi.org/project/transpark/)
[![Python >=3.10](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 🚀 Overview

**Transpark** is a lightweight and extensible framework that helps you compose complex transformation pipelines and cache intermediate results in PySpark — declaratively and reproducibly.

It is especially useful for building **data pipelines**, **ETL flows**, and **intermediate result debugging**.

Except composable pattern it also provides useful operations like suffixed joins or customized window functions

---

## 📦 Features

- ✅ Decorator-based transformation definitions
- ✅ Caching of intermediate steps via `@transformation(cache=True)`
- ✅ Pluggable `ComposableDFModel` for ordered execution
- ✅ `CachableDFModel` for memory-efficient temporary caching
- ✅ Extensible mixin (`TransparkMixin`) to bring it all together

---