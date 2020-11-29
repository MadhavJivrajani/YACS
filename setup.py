import setuptools

setuptools.setup(
	name='yacs',
	version='0.0.1',
	scripts=['./scripts/yacs'],
	author='nap-reduce',
	description='A tool for simulating centralized scheduling policies',
	packages=['yacs', 'yacs/component', 'yacs/component/utils'],
	install_requires=[
		'setuptools',
		'numpy',
		'matplotlib',
	],
	python_requires='>=3.8',
	entry_points = {
        'console_scripts': ['yacs=yacs.cli:main'],
    }
)
