import lib.config
import tools.portaldb as portaldb
import os
import shlex
import subprocess
import time
import re


class MPI_Plugin():

    def __init__(self, current_dir):
        self.uri = lib.config.ConfigDict()
        self.machines = "machines"  # name of machines file
        self.jobs_id = []
        self.app_dir = current_dir   # initial directory where the application
        # which used the plugin is at.
        self.mpiexec_dir = None
        self.mpi_submitter = self.uri.get('processing', 'mpi_submitter')
        self.instance = self.uri.get('project', 'instance')
        self.mpi_hosts = self.uri.get('processing', 'mpi_hosts')
        self.n_mpi_hosts = len(self.mpi_hosts.split(','))

        self.create_machines_file()
        self.allocate_resources()

    def create_machines_file(self):
        """Creates the machines file based on the user who calls.
        The number of machines to be added is fixed by now.
        """
        # [CAS]: I have two options here, keep the condor_findhost searching the
        # better machines to run mpi jobs, which is a good option because the
        # machines will be alocated using an internal condor ranking, OR declare
        # the MPI dedicated machines in the ini file, in this case I have to
        # guarantee that those machines aren't running any job because they will
        # reserved as 'WholeMachine'.
        #rcode = 1
        # while (rcode > 0):
        #    fmach = open(self.machines, "w")
        #    cmd = "condor_findhost -n %s -m" %(self.n_mpi_hosts)
        #    #cmd = "condor_status -avail -format "%s\n" Name"
        #    cmd = shlex.split(cmd)
        #    res = subprocess.Popen(cmd,stdin=subprocess.PIPE, stdout=fmach, stderr=subprocess.PIPE)
        #    res.wait()
        #    rcode = res.returncode
        #    fmach.close()
        fmach = open(self.machines, "w")
        for host in self.mpi_hosts.split(','):

            # production-rocks ncs dont use the sufix linea.gov.br
            if self.instance == 'production':
                fqdn = host + '.local\n'
            else:
                fqdn = host + '.linea.gov.br\n'

            fmach.write(fqdn)

        fmach.close()

    def allocate_resources(self):
        """Occupies the machines identified by create_machines_file()
        with dummy job (sleep) so that when the mpi application finishes
        those dummy jobs are removed. This is to guarantee that they 
        will be reserved to the MPI application.

        Input:

        Output:
        """
        # Allocating the machines in Condor
        fmach = open(self.machines, "r")
        self.jobs_id = []
        machines_list = fmach.readlines()
        for machine in machines_list:
            machine = machine.replace('\n', '')
            print "-"+machine+"-"

            subfile_txt = """
            universe    = vanilla
            executable  = /bin/sleep
            arguments   = 3d
            requirements = Machine == "%s"
            +RequiresWholeMachine = True 
            error = condor.$(Process).err
            output = condor.$(Process).out
            log = condor.$(Process).log
            should_transfer_files=yes
            when_to_transfer_output = ON_EXIT_OR_EVICT
            queue 1
            """ % (machine)

            subfile = open('submit.job', 'w')
            subfile.write(subfile_txt)
            subfile.close()

            cmd = "condor_submit -remote %s submit.job" % self.mpi_submitter
            cmd = shlex.split(cmd)
            sts = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            retorno = sts.communicate()
            m = re.search('(?<=submitted to cluster) [0-9]*', retorno[0])
            condor_id = m.group(0).split()[0]
            print condor_id
            self.jobs_id.append(int(condor_id))
        fmach.close()

    def set_mpirun_dir(self, prefix=None):
        """ Sets and creates the directory where the MPI application
        will run"""
        self.mpiexec_dir = prefix+"/mpiexec"
        os.mkdir(self.mpiexec_dir)

    def exec_mpirun(self, number_procs, cmd):
        """Executes cmd via mpirun, with number of processes
        """
        os.chdir(self.mpiexec_dir)
        cmd = 'mpirun -n %s -envall -machinefile %s %s' % (
            number_procs, self.machines, cmd)
        cmd = shlex.split(cmd)
        fmod_out = open("mpi_mod.out", "w")
        fmod_err = open("mpi_mod.err", "w")
        process = subprocess.Popen(cmd, stdout=fmod_out, stderr=fmod_err)
        process.wait()
        ret_code = process.returncode
        fmod_err.close()
        fmod_out.close()

        self.vacate_resources()

        os.chdir(self.app_dir)

        return ret_code

    def vacate_resources(self):
        """Vacating machines used
        """
        for jb_id in self.jobs_id:
            cmd = "condor_rm -name devel2 %s" % jb_id
            cmd = shlex.split(cmd)
            sts = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output = sts.communicate()
            print output

    def copy_input_files(self, inputs=[]):
        """Copy input files to the mpi directory created for the 
        application to run.

        Input:
          inputs: list of dicts containning the path and name of files 
        """
        for file_name, file_path in inputs:
            os.system('mv %s %s' % (file_path+os.sep+file_name,
                                    self.mpiexec_dir+os.sep+file_name))
        # also copies the machines file
        os.system('mv %s %s' % (self.app_dir+os.sep+"machines",
                                self.mpiexec_dir+os.sep+"machines"))

    def copy_output_files(self, outputs=[]):
        """Copy output files to back to the application directory.
        The path to copy to is considered the application initial path.

        Input:
          outputs: list containning the name of expected output files 
        """
        for file in outputs:
            os.system('mv %s %s' %
                      (self.mpiexec_dir+os.sep+file, self.app_dir+os.sep+file))
