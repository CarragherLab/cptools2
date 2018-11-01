"""
Generate eddie submission scripts for the
staging, analysis and destaging jobs
"""

from __future__ import print_function
import os
import textwrap
from datetime import datetime
import yaml
from scissorhands import script_generator
from cptools2 import utils
from cptools2 import colours
from cptools2.colours import pretty_print


def make_command_paths(commands_location):
    """
    create the paths to the commands
    """
    names = ["staging", "cp_commands", "destaging"]
    return {name: os.path.join(commands_location, name+".txt") for name in names}


def _lines_in_commands(staging, cp_commands, destaging):
    """
    Number of lines in each of the commands file.
    While the number of lines in each of the files *should* be the same,
    it's worth checking.

    Parameters:
    -----------
    staging: string
        path to staging commands

    cp_commands: string
        path to cellprofiler commands

    destaging: string
        path to destaging commands

    Returns:
    --------
    Dictionary, e.g:
        {staging: 128, cp_commands: 128, destaging: 128}
    """
    names = ["staging", "cp_commands", "destaging"]
    paths = [staging, cp_commands, destaging]
    counts = [utils.count_lines_in_file(i) for i in paths]
    # check if the counts differ
    if len(set(counts)) > 1:
        raise RuntimeWarning("command files contain differing number of lines")
    return {name: count for name, count in zip(names, counts)}


def lines_in_commands(commands_location):
    """
    Given a path to a directory which contains the commands:
        1. staging
        2. cellprofiler commands
        3. destaging
    This will return the number of lines in each of these text files.

    Parameters:
    -----------
    commands_location: string
        path to directory containing commands

    Returns:
    ---------
    Dictionary

        {"staging":     int,
         "cp_commands": int,
         "destaging":   int}
    """
    command_paths = make_command_paths(commands_location)
    return _lines_in_commands(**command_paths)


def load_module_text():
    """returns load module commands"""
    return textwrap.dedent(
        """
        module load anaconda/5.0.0.1
        source activate cellprofiler
        """
    )


def make_qsub_scripts(commands_location, commands_count_dict, logfile_location):
    """
    Create and save qsub submission scripts in the same location as the
    commands.

    Parameters:
    -----------
    commands_location: string
        path to directory that contains staging, cp_commands, and destaging
        command files.

    commands_count_dict: dictionary
        dictionary of the number of commands contain in each of the jobs

    logfile_location: string
        where to store the log files. By default this will store them
        in a directory alongside the results.


    Returns:
    ---------
    Nothing, writes files to `commands_location`
    """
    cmd_path = make_command_paths(commands_location)
    time_now = datetime.now().replace(microsecond=0)
    time_now = str(time_now).replace(" ", "-")
    # append random hex to job names - this allows you to run multiple jobs
    # without the -hold_jid flags fron clashing
    job_hex = script_generator.generate_random_hex()
    n_tasks = commands_count_dict["cp_commands"]
    # FIXME: using AnalysisScript class for everything, due to the 
    #        {Staging, Destaging}Script class not having loop_through_file
    stage_script = BodgeScript(
        name="staging_{}".format(job_hex),
        memory="1G",
        output=os.path.join(logfile_location, "staging"),
        tasks=commands_count_dict["staging"]
    )
    stage_script += "#$ -q staging\n"
    # limit staging node requests
    stage_script += "#$ -p -500\n"
    stage_script += "#$ -tc 20\n"
    stage_script.bodge_array_loop(phase="staging",
                                  input_file=cmd_path["staging"])
    stage_loc = os.path.join(commands_location,
                             "{}_staging_script.sh".format(time_now))
    stage_script.save(stage_loc)
    analysis_script = script_generator.AnalysisScript(
        name="analysis_{}".format(job_hex),
        tasks=n_tasks,
        hold_jid_ad="staging_{}".format(job_hex),
        pe="sharedmem 1",
        memory="12G",
        output=os.path.join(logfile_location, "analysis")
    )
    analysis_script += load_module_text()
    analysis_script.loop_through_file(cmd_path["cp_commands"])
    analysis_loc = os.path.join(commands_location,
                                "{}_analysis_script.sh".format(time_now))
    analysis_script += make_logfile_text(logfile_location,
                                         job_file=job_hex,
                                         n_tasks=n_tasks)
    analysis_script.save(analysis_loc)
    destaging_script = BodgeScript(
        name="destaging_{}".format(job_hex),
        memory="1G",
        hold_jid_ad="analysis_{}".format(job_hex),
        tasks=commands_count_dict["destaging"],
        output=os.path.join(logfile_location, "destaging")
    )
    destaging_script.bodge_array_loop(phase="destaging",
                                      input_file=cmd_path["destaging"])
    destage_loc = os.path.join(commands_location,
                               "{}_destaging_script.sh".format(time_now))
    destaging_script.save(destage_loc)
    # create script to submit staging, analysis and destaging scripts
    submit_script = make_submit_script(commands_location, time_now)
    pretty_print("saving master submission script at {}".format(colours.yellow(submit_script)))
    utils.make_executable(submit_script)


def make_logfile_text(logfile_location, job_file, n_tasks):
    text = """
    # get the exit code from the cellprofiler job
    RETURN_VAL=$?

    if [[ $RETURN_VAL == 0 ]]; then
        RETURN_STATUS="Finished"
    else
        RETURN_STATUS="Failed with error code: $RETURN_VAL"
    fi

    LOG_FILE_LOC={logfile_location}/{job_file}.log
    echo "`date +"%Y-%m-%d %H:%M"`  "$JOB_ID"  "$SGE_TASK_ID"  "$RETURN_STATUS"" >> "$LOG_FILE_LOC"
    """.format(logfile_location=logfile_location,
               job_file=job_file,
               n_tasks=n_tasks)
    return textwrap.dedent(text)


def make_submit_script(commands_location, job_date):
    """
    Create a shell script which will submit the staging, analysis and
    destaging scripts.

    Parameters:
    -----------
    commands_location: string
        path to where the commands are stored
    job_date: string
        date for the submission scripts

    Returns:
    --------
    path to submit_script
    also writes script to disk in `commands_location`.
    """
    # create full paths to the generated scripts
    names = ["staging", "analysis", "destaging"]
    script_dict = {}
    for name in names:
        script_name = "{}_{}_script.sh".format(job_date, name)
        script_path = os.path.join(commands_location, script_name)
        script_dict[name] = script_path
    # create text for a shell script that qsub's the scripts
    output = """
             #!/bin/sh

             # This script submits the staging, analysis and destaging
             # scripts in the correct order

             # NOTE: run this as a shell script, NOT a submission script
             # so either call `./name_of_script` or `bash name_of_script`

             qsub {staging_script}
             qsub {analysis_script}
             qsub {destaging_script}
            """.format(staging_script=script_dict["staging"],
                       analysis_script=script_dict["analysis"],
                       destaging_script=script_dict["destaging"])
    save_location = "{}/{}_SUBMIT_JOBS.sh".format(commands_location, job_date)
    # save this shell script and return it's path
    with open(save_location, "w") as f:
        f.write(textwrap.dedent(output))
    return save_location


class BodgeScript(script_generator.AnalysisScript):
    """
    Whilst trying to fix rsync issues with filepaths containing spaces,
    the rsync commands stopped working when called from `$SEED`, though
    will work if saved as a single command in a shell script, and then calling
    bash on that script.

    So this class inherits scissorhands.script_generator.AnalsisScript, but
    adds an extra method which should be used instead of .loop_through_file().
    """

    def __init__(self, *args, **kwargs):
        script_generator.AnalysisScript.__init__(self, *args, **kwargs)

    def bodge_array_loop(self, phase, input_file):
        """
        As a temporary fix (hopefully), this method can work instead of
        scissorhands.script_generator.AnalysisScript.loop_through_file()

        Parameters:
        -----------
        phase: string
            prefix of the hidden commands file, e.g "staging" or "destaging"
        input_file: string
            path to a file. This file should contain multiple lines of commands.
            Each line will be run separately in an array job.

        Returns:
        ---------
        nothing, adds text to template
        """
        text = textwrap.dedent(
            """
            SEEDFILE="{input_file}"
            SEED=$(awk "NR==$SGE_TASK_ID" "$SEEDFILE")
            # create shell script from single command, run, then delete
            echo "$SEED" > .{phase}_"$JOB_ID"_"$SGE_TASK_ID".sh
            bash .{phase}_"$JOB_ID"_"$SGE_TASK_ID".sh
            rm .{phase}_"$JOB_ID"_"$SGE_TASK_ID".sh
            """.format(phase=phase, input_file=input_file)
        )
        self.template += text

