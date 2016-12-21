import yaml
from jinja2 import Environment, FileSystemLoader

def build_from_template(inputs, template_file, file_name):
    jinja = Environment(loader = FileSystemLoader('templates'))
    template = jinja.get_template(template_file)
    template.stream(inputs).dump(file_name)


def load_config(config_file):
    with open(config_file, 'r') as config_file:
        return yaml.safe_load(config_file)