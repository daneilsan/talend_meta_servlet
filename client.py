__author__ = 'esdga'
import requests
import base64
import json


class Client():
    def _get_uri(self, data):
        """
            data = python native data
        :return:
        str
        """

        b_data = base64.b64encode(json.dumps(data).encode('utf-8'))
        str_data = b_data.decode("utf-8")

        uri = "http://{host}:{port}/{tacname}/metaServlet?{data}".format(
            host=self.host,
            port=self.port,
            tacname=self.tacname,
            data=str_data
        )

        return uri

    def __init__(self, user, password, host, port=None, webapp=None):
        self.user = user
        self.password = password
        self.host = host
        self.port = port if port else 8080
        self.tacname = webapp if webapp else "org.talend.administrator"

    def start(self, task_id, context=None):
        """
        Starts a job asyncronously
        :param task_id: job's number
        :param context: dictionary with the context parameters
        :return: result
        """
        data = {
            "actionName": "runTask",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id,
            "mode": "asynchronous"
        }
        if context:
            data["context"] = context
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def run_and_wait(self, task_id, context=None):
        """
        Starts a job and waits until it is finished
        :param task_id: job's number
        :param context: dictionary with the context parameters
        :return: result
        """
        data = {
            "actionName": "runTask",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id,
            "mode": "synchronous"
        }
        if context:
            data["context"] = context
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def get_task_status(self, task_id):
        """
        Get the status of the task
        :param task_id: job's number
        :param context: dictionary with the context parameters
        :return: result
        """
        data = {
            "actionName": "getTaskStatus",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def add_job_server(self, host, name, password, username, command_port=8000, file_port=8001, monitoring_port=8888):
        data = {
            "actionName": "addServer",
            "authUser": self.user,
            "authPass": self.password,
            "commandPort": command_port,
            "filePort": file_port,
            "host": host,
            "monitoringPort": monitoring_port,
            "name": name,
            "password": password,
            "username": username
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def remove_job_server(self, server_id):
        data = {
            "actionName": "removeServer",
            "authUser": self.user,
            "authPass": self.password,
            "id": server_id,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def create_user(self, first_name, last_name, login, password, role, svn_login, svn_password):
        """

        :param first_name:
        :param last_name:
        :param login:
        :param password:
        :param role: administrator|designer
        :param svn_login:
        :param svn_password:
        :return:
        """
        data = {
            "actionName": "createUser",
            "authUser": self.user,
            "authPass": self.password,
            "userFirstName": first_name,
            "userLastName": last_name,
            "userLogin": login,
            "userPassword": password,
            "userRole": role,
            "userSvnLogin": svn_login,
            "userSvnPassword": svn_password
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def delete_user(self, first_name, last_name, login):
        data = {
            "actionName": "deleteUser",
            "authUser": self.user,
            "authPass": self.password,
            "userLogin": login,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def create_project(self, project_name, project_svn_location, project_author_login, force_svn_folder_creation, project_description=None):
        """

        :param project_name:
        :param project_svn_location:
        :param project_author_login:
        :param force_svn_folder_creation: true|false
        :param project_description:
        :return:
        """
        data = {
            "actionName": "createProject",
            "authUser": self.user,
            "authPass": self.password,
            "forceSvnFolderCreation": force_svn_folder_creation,
            "projectName": project_name,
            "projectSvnLocation": project_svn_location,
            "projectAuthorLogin": project_author_login,
            "projectLanguage": "java"
        }
        if project_description:
            data["projectDescription"] = project_description
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def delete_project(self, project_name):
        data = {
            "actionName": "deleteProject",
            "authUser": self.user,
            "authPass": self.password,
            "projectName": project_name,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def create_authorization(self, user, auth, project):
        """

        :param user:
        :param auth: ReadWrite
        :param project:
        :return:
        """
        data = {
            "actionName": "createAuthorization",
            "authUser": self.user,
            "authPass": self.password,
            "authorizationProjectName": project,
            "authorizationType": auth,
            "authorizationUserLogin": user
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def project_exist(self, project):
        data = {
            "actionName": "projectExist",
            "authUser": self.user,
            "authPass": self.password,
            "projectName": project
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def user_exist(self, user):
        data = {
            "actionName": "userExist",
            "authUser": self.user,
            "authPass": self.password,
            "userLogin": user
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def get_cmd_line_info(self):
        data = {
            "actionName": "getCmdLineInfo",
            "authUser": self.user,
            "authPass": self.password,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def list_connection(self):
        data = {
            "actionName": "listConnection",
            "authUser": self.user,
            "authPass": self.password,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def get_archiva_url(self):
        data = {
            "actionName": "getArchivaUrl",
            "authUser": self.user,
            "authPass": self.password,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def create_branch(self, project_name, source_name, target_name):
        data = {
            "actionName": "createBranch",
            "projectName": project_name,
            "source": source_name,
            "target": target_name,
            "authUser": self.user,
            "authPass": self.password,
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def create_task(self, task_name, project_name, job_name,
                    context_name, execution_server_name, active=None,
                    job_version=None, regenerate_on_change=None, context_to_children=None,
                    description=None, branch=None, on_unknown_state_job=None,
                    add_statistics_enabled=None, exec_statistics_enabled=None):
        data = {
            "actionName": "createTask",
            "authUser": self.user,
            "authPass": self.password,
            "taskName": task_name,
            "projectName": project_name,
            "jobName": job_name,
            "contextName": context_name,
            "executionServerName": execution_server_name
        }
        if active:
            data["active"] = active
        else:
            data["active"] = True
        if exec_statistics_enabled:
            data["execStatisticsEnabled"] = exec_statistics_enabled
        else:
            data["execStatisticsEnabled"] = exec_statistics_enabled
        if add_statistics_enabled:
            data["addStatisticsCodeEnabled"] = add_statistics_enabled
        else:
            data["addStatisticsCodeEnabled"] = add_statistics_enabled
        if job_version:
            data["jobVersion"] = job_version
        else:
            data["jobVersion"] = "Latest"
        if context_to_children:
            data["applyContextToChildren"] = context_to_children
        else:
            data["applyContextToChildren"] = False
        if regenerate_on_change:
            data["regenerateJobOnChange"] = regenerate_on_change
        else:
            data["regenerateJobOnChange"] = False
        if description:
            data["description"] = on_unknown_state_job
        else:
            data["description"] = ""
        if on_unknown_state_job:
            data["onUnknownStateJob"] = on_unknown_state_job
        else:
            data["onUnknownStateJob"] = "KILL_TASK"
        if branch:
            data["branch"] = branch
        else:
            data["branch"] = "trunk"
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def update_task(self, task_name, project_name, job_name,
                    context_name, execution_server_name, active=None,
                    job_version=None, regenerate_on_change=None, context_to_children=None,
                    description=None, branch=None, on_unknown_state_job=None,
                    add_statistics_enabled=None, exec_statistics_enabled=None):
        data = {
            "actionName": "updateTask",
            "authUser": self.user,
            "authPass": self.password,
            "taskName": task_name,
            "projectName": project_name,
            "jobName": job_name,
            "contextName": context_name,
            "executionServerName": execution_server_name
        }
        if active:
            data["active"] = active
        else:
            data["active"] = True
        if exec_statistics_enabled:
            data["execStatisticsEnabled"] = exec_statistics_enabled
        else:
            data["execStatisticsEnabled"] = exec_statistics_enabled
        if add_statistics_enabled:
            data["addStatisticsCodeEnabled"] = add_statistics_enabled
        else:
            data["addStatisticsCodeEnabled"] = add_statistics_enabled
        if job_version:
            data["jobVersion"] = job_version
        else:
            data["jobVersion"] = "Latest"
        if context_to_children:
            data["applyContextToChildren"] = context_to_children
        else:
            data["applyContextToChildren"] = False
        if regenerate_on_change:
            data["regenerateJobOnChange"] = regenerate_on_change
        else:
            data["regenerateJobOnChange"] = False
        if description:
            data["description"] = on_unknown_state_job
        else:
            data["description"] = ""
        if on_unknown_state_job:
            data["onUnknownStateJob"] = on_unknown_state_job
        else:
            data["onUnknownStateJob"] = "KILL_TASK"
        if branch:
            data["branch"] = branch
        else:
            data["branch"] = "trunk"
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

#todo esto esta mal
    def get_task_related_jobs(self, task_id):
        data = {
            "actionName": "getTasksRelatedToJobs",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

#todo esto esta mal
    def request_generate_task(self, task_id):
        data = {
            "actionName": "requestGenerate",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

#todo esto esta mal
    def request_deploy_task(self, task_id):
        data = {
            "actionName": "requestDeploy",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def request_run_task(self, task_id):
        data = {
            "actionName": "requestRun",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def request_stop_task(self, task_id):
        data = {
            "actionName": "requestStop",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def request_pause_task_triggers(self, task_id):
        data = {
            "actionName": "requestPauseTriggers",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def request_resume_task_triggers(self, task_id):
        data = {
            "actionName": "requestResumeTriggers",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def pause_task(self, task_id):
        data = {
            "actionName": "pauseTask",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def stop_task(self, task_id):
        data = {
            "actionName": "stopTask",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def get_task_log(self, task_id):
        data = {
            "actionName": "taskLog",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def get_task_by_name(self, name):
        data = {
            "actionName": "getTaskIdByName",
            "authUser": self.user,
            "authPass": self.password,
            "label": name
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)

    def get_task_by_id(self, task_id):
        data = {
            "actionName": "getNameByTaskId",
            "authUser": self.user,
            "authPass": self.password,
            "taskId": task_id
        }
        r = requests.get(self._get_uri(data))
        return json.loads(r.text)