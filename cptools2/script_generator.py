"""
Automatically generate SGE submission scripts from a template.
"""
import random
from string import Template


class SGEScript(object):
    """
    Base class for SGE submission script.
    """

    def __init__(self, name=None, memory="2G", run_time="06:00:00"):
        if name is None:
            name = _generate_random_hex()

        self.template = Template(
            """
            #!/bin/bash

            #$ -N %name
            #$ -l h_vmem=%memory
            #$ -l h_rt=%run_time
            #$ -j y

            . /etc/profiles.d/modules.sh
            """).substitute(name=name, memory=memory, run_time=run_time)



class StagingScript(SGEScript):
    # TODO
    pass


class AnalysisScript(SGEScript):
    # TODO
    pass


class DestagingScript(SGEScript):
    # TODO
    pass


def _generate_random_hex():
    all = "0123456789abcdef"
    result = [random.choice('abcdef')] + [random.choice(all) for _ in range(4)]
    random.shuffle(result)
    result.insert(0, random.choice(all[1:]))
    return ''.join(result)
