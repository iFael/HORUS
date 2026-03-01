from setuptools import setup, find_packages

setup(
    name="horus",
    version="1.0.0",
    description="Sistema de análise de risco em dados públicos brasileiros",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="HORUS Contributors",
    license="MIT",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0",
        "networkx>=3.0",
        "requests>=2.31",
        "tqdm>=4.65",
        "tenacity>=8.2",
        "rich>=13.0",
        "streamlit>=1.30",
        "pyvis>=0.3",
        "python-dotenv>=1.0",
        "thefuzz>=0.20",
        "plotly>=5.18",
        "apscheduler>=3.10",
    ],
    extras_require={
        "dev": ["pytest>=7.0", "responses>=0.23", "ruff"],
    },
    entry_points={
        "console_scripts": [
            "horus=horus.cli:main",
        ],
    },
)
