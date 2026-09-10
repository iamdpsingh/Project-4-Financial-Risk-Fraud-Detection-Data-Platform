import setuptools

setuptools.setup(
    name='financial_risk_platform',
    version='1.0',
    install_requires=[
        'apache-beam[gcp]>=2.55.0',
        'google-cloud-storage>=2.14.0',
        'python-dotenv>=1.0.0',
        'pandas>=2.0.0'
    ],
    packages=setuptools.find_packages(),
)
