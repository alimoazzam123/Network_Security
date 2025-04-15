from setuptools import setup, find_packages
from typing import List

def get_requirements(file_path: str) -> List[str]:
    """
    This function will return the list of requirements
    """
    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n", "") for req in requirements]
        if "-e ." in requirements:
            requirements.remove("-e .")
    return requirements

REQUIREMENTS_FILE_NAME = "requirements.txt"
requirements = get_requirements(REQUIREMENTS_FILE_NAME) 

setup(
    name="src",
    version="0.0.1",
    author="Md Moazzam Ali",
    author_email="mdmoazzamali984@gmail.com",
    packages=find_packages(),
    install_requires=requirements
)
