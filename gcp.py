#!/usr/bin/python3
import os
import time
import googleapiclient.discovery
from google.oauth2 import service_account
from configparser import ConfigParser

parser = ConfigParser()
parser.read('config.ini')


class GCP:
    def __init__(self):
        self.compute = googleapiclient.discovery.build(
            'compute',
            'v1',
            credentials=service_account.Credentials.from_service_account_file(
                parser.get('gcp', 'sa'),
                scopes=['https://www.googleapis.com/auth/cloud-platform']))

    # [START create_instance]
    def create_instance(self, project, zone, name, startup_script):
        machine_type = "zones/" + zone + "/machineTypes/" + parser.get(
            'gcp', 'machine_type')

        # Get the latest Debian Jessie image.
        image_response = self.compute.images().getFromFamily(
            project=parser.get('gcp', 'vm-image-project'),
            family=parser.get('gcp', 'vm-image-family'),
        ).execute()
        source_disk_image = image_response['selfLink']
        startup_script = open(startup_script, 'r').read()

        config = {
            'name':
            name,
            'machineType':
            machine_type,
            'scheduling': {
                'preemptible': True
            },

            # Specify the boot disk and the image to use as a source.
            'disks': [{
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network':
                'global/networks/default',
                'accessConfigs': [{
                    'type': 'ONE_TO_ONE_NAT',
                    'name': 'External NAT'
                }]
            }],

            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email':
                'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],

            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }]
            }
        }
        while True:
            try:
                operation = self.compute.instances().insert(
                    project=project, zone=zone, body=config).execute()
                waitResponse = self.wait_for_operation(project, zone,
                                                       operation["name"])
                IP = self.get_IP_address(project, zone, name)
                return IP
            except:
                continue

    # [END create_instance]

    # [START delete_instance]
    def delete_instance(self, project, zone, name):

        return self.compute.instances().delete(project=project,
                                               zone=zone,
                                               instance=name).execute()

    # [END delete_instance]

    # [START wait_for_operation]
    def wait_for_operation(self, project, zone, operation):
        while True:
            result = self.compute.zoneOperations().get(
                project=project, zone=zone, operation=operation).execute()
            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)

# [END wait_for_operation

    def get_IP_address(self, project, zone, name):
        try:
            if (self.isInstanceRunning(project, zone, name)):
                instance = self.compute.instances().get(
                    project=project, zone=zone, instance=name).execute()
                ext_ip = instance['networkInterfaces'][0]['accessConfigs'][0][
                    'natIP']
                return ext_ip
            else:
                self.startInstance(project, zone, name)
        except Exception as e:
            raise e

    def isInstanceRunning(self, project, zone, name):
        try:
            status = self.compute.instances().get(project=project,
                                                  zone=zone,
                                                  instance=name).execute()

            if status['status'] == "RUNNING":
                return True
            else:
                return False
        except Exception as e:
            raise e

    def startInstance(self, project, zone, name):
        try:
            operation = self.compute.instances().start(
                project=project, zone=zone, instance=name).execute()
            waitResponse = self.wait_for_operation(project, zone,
                                                   operation["name"])
            return True
        except Exception as e:
            raise e


# g = GCP()
# print(g.get_IP_address("milan-chheta", "us-central1-c", "keyvalue-node"))
# # print(g.startInstance("milan-chheta", "us-central1-c", "keyvalue-node"))
