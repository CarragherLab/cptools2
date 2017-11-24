"""
- use scissorhands.check_output.Qacct(analysis_job_hex)
- find failed tasks
- create re-run tasks commands
- create re-run task scripts
- increase h_vmem (usual reason for failure) 
    - can check if Qacct.max_vmem >= h_vmem
"""

import scissorhands


class Sweeper(scissorhands.check_output.Qacct):
    """
    scissorhands.check_output.Qacct class with some extras, mainly:
        - exceeded memory
            - check if failure was due to max_vmem exceeding h_vmem
        - exceeded time limit
            - check if failure was due to wallclock time exceeding h_rt
    """

    def __init__(self, h_vmem, h_rt, *args, **kwargs):
        scissorhands.check_output.Qacct.__init__(self, *args, **kwargs)
        self.h_vmem = self.parse_vmem(h_vmem)
        self.h_rt = self.parse_wallclock(h_rt)
        self.failure_dict = reason_for_failure

    def reason_for_failure(self):
        """
        create dictionary of failed tasks, and their reason for failure.

        Returns:
        ---------
        Dictionary:
            {task_id: "reason_string"}
        """
        failure_dictionary = {}
        for task_id, task_dict in self.qacct_dict.items():
            if task_dict.get("exit_status") != "0":
                # if a task failed then parse it's runtime and memory useage
                max_vmem = self.parse_vmem(task_dict.get("max_vmem"))
                max_runtime = int((task_dict.get("ru_wallclock")))
                if max_vmem >= self.h_vmem:
                    failure_reason = "out-of-memory"
                elif max_runtime >= self.h_rt:
                    failure_reason = "out-of-time"
                else:
                    failure_reason = "unknown"
                failure_dictionary.update({task_id: failure_reason})
        return failure_dictionary

    @staticmethod
    def parse_rt(runtime):
        """
        convert time in format hours:minutes:seconds into seconds

        Parameters:
        -----------
        h_rt: string

        Returns:
        --------
        integer: time in seconds
        """
        h, m, s = runtime.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)

    @staticmethod
    def parse_vmem(vmem):
        """
        converts vmem string into a float.
        e.g '12.2G' => 12.2

        Parameters:
        -----------
        vmem: string

        Returns:
        ---------
        float vmem as a floating point number with no suffix
        """
        if vmem[-1] != "G":
            raise RuntimeError("Unexpected suffix in vmem string, only ",
                               "accepts values ending with 'G' (blame Scott)")
        return float("".join([i for i in vmem if not i.isalpha()]))





