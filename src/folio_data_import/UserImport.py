import argparse
import asyncio
import datetime
import getpass
import json
import os
import time
from pathlib import Path

import aiofiles
import folioclient
import httpx

try:
    utc = datetime.UTC
except AttributeError:
    import zoneinfo

    utc = zoneinfo.ZoneInfo("UTC")


class UserImporter(object):
    """
    Class to import users from a JSON-lines file into FOLIO
    """

    def __init__(
        self,
        folio_client: folioclient.FolioClient,
        library_name: str,
        user_file_path: Path,
        batch_size: int,
        limit_simultaneous_requests: asyncio.Semaphore,
        logfile,
        errorfile,
        http_client: httpx.AsyncClient,
        only_update_present_fields: bool = False,
    ) -> None:
        self.limit_simultaneous_requests = limit_simultaneous_requests
        self.batch_size = batch_size
        self.folio_client = folio_client
        self.library_name = library_name
        self.user_file_path = user_file_path
        self.logfile = logfile
        self.errorfile = errorfile
        self.http_client = http_client
        self.only_update_present_fields = only_update_present_fields
        self.lock = asyncio.Lock()
        self.logs: dict = {"created": 0, "updated": 0, "failed": 0}

    async def do_import(self):
        """
        Main method to import users.

        This method triggers the process of importing users by calling the `process_file` method.
        """
        await self.process_file()

    async def get_existing_user(self, user_obj):
        """
        Retrieves an existing user from FOLIO based on the provided user object.

        Args:
            user_obj: The user object containing the information to match against existing users.

        Returns:
            The existing user object if found, otherwise an empty dictionary.
        """
        match_key = "id" if ("id" in user_obj) else "externalSystemId"
        try:
            existing_user = await self.http_client.get(
                self.folio_client.okapi_url + "/users",
                headers=self.folio_client.okapi_headers,
                params={"query": f"{match_key}=={user_obj[match_key]}"},
            )
            existing_user.raise_for_status()
            existing_user = existing_user.json().get("users", [])
            existing_user = existing_user[0] if existing_user else {}
        except httpx.HTTPError:
            existing_user = {}
        return existing_user

    async def get_existing_rp(self, user_obj, existing_user):
        """
        Retrieves the existing request preferences for a given user.

        Args:
            user_obj (dict): The user object.
            existing_user (dict): The existing user object.

        Returns:
            dict: The existing request preferences for the user.
        """
        try:
            existing_rp = await self.http_client.get(
                self.folio_client.okapi_url
                + "/request-preference-storage/request-preference",
                headers=self.folio_client.okapi_headers,
                params={
                    "query": f"userId=={existing_user.get('id', user_obj.get('id', ''))}"
                },
            )
            existing_rp.raise_for_status()
            existing_rp = existing_rp.json().get("requestPreferences", [])
            existing_rp = existing_rp[0] if existing_rp else {}
        except httpx.HTTPError:
            existing_rp = {}
        return existing_rp

    async def get_existing_pu(self, user_obj, existing_user):
        """
        Retrieves the existing permission user for a given user.

        Args:
            user_obj (dict): The user object.
            existing_user (dict): The existing user object.

        Returns:
            dict: The existing permission user object.
        """
        try:
            existing_pu = await self.http_client.get(
                self.folio_client.okapi_url + "/perms/users",
                headers=self.folio_client.okapi_headers,
                params={
                    "query": f"userId=={existing_user.get('id', user_obj.get('id', ''))}"
                },
            )
            existing_pu.raise_for_status()
            existing_pu = existing_pu.json().get("permissionUsers", [])
            existing_pu = existing_pu[0] if existing_pu else {}
        except httpx.HTTPError:
            existing_pu = {}
        return existing_pu

    async def map_address_types(self, user_obj, address_type_map):
        """
        Maps the address type IDs in the user object to the corresponding values in the address_type_map.

        Args:
            user_obj (dict): The user object containing personal information.
            address_type_map (dict): A dictionary mapping address type IDs to their corresponding values.

        Returns:
            None

        Raises:
            KeyError: If an address type ID in the user object is not found in the address_type_map.

        """
        if "personal" in user_obj and "addresses" in user_obj["personal"]:
            for address in user_obj["personal"]["addresses"]:
                try:
                    address["addressTypeId"] = address_type_map[
                        address["addressTypeId"]
                    ]
                except KeyError:
                    if address["addressTypeId"] not in address_type_map.values():
                        print(
                            f"Address type {address['addressTypeId']} not found, removing address"
                        )
                        await self.logfile.write(
                            f"Address type {address['addressTypeId']} not found, removing address\n"
                        )
                        del address
            if len(user_obj["personal"]["addresses"]) == 0:
                del user_obj["personal"]["addresses"]

    async def map_patron_groups(self, user_obj, patron_group_map):
        """
        Maps the patron group of a user object using the provided patron group map.

        Args:
            user_obj (dict): The user object to update.
            patron_group_map (dict): A dictionary mapping patron group names.

        Returns:
            None
        """
        try:
            user_obj["patronGroup"] = patron_group_map[user_obj["patronGroup"]]
        except KeyError:
            if user_obj["patronGroup"] not in patron_group_map.values():
                print(
                    f"Patron group {user_obj['patronGroup']} not found, removing patron group"
                )
                await self.logfile.write(
                    f"Patron group {user_obj['patronGroup']} not found in, removing patron group\n"
                )
                del user_obj["patronGroup"]

    async def update_existing_user(self, user_obj, existing_user):
        """
        Updates an existing user with the provided user object.

        Args:
            user_obj (dict): The user object containing the updated user information.
            existing_user (dict): The existing user object to be updated.

        Returns:
            tuple: A tuple containing the updated existing user object and the response from the API call.

        Raises:
            None

        """
        if self.only_update_present_fields:
            new_personal = user_obj.pop("personal", {})
            existing_personal = existing_user.pop("personal", {})
            existing_preferred_first_name = existing_personal.pop(
                "preferredFirstName", ""
            )
            existing_addresses = existing_personal.get("addresses", [])
            existing_user.update(user_obj)
            existing_personal.update(new_personal)
            if (
                not existing_personal.get("preferredFirstName", "")
                and existing_preferred_first_name
            ):
                existing_personal["preferredFirstName"] = existing_preferred_first_name
            if not existing_personal.get("addresses", []):
                existing_personal["addresses"] = existing_addresses
            if existing_personal:
                existing_user["personal"] = existing_personal
        else:
            existing_user.update(user_obj)
        create_update_user = await self.http_client.put(
            self.folio_client.okapi_url + f"/users/{existing_user['id']}",
            headers=self.folio_client.okapi_headers,
            json=existing_user,
        )
        return existing_user, create_update_user

    async def create_new_user(self, user_obj):
        """
        Creates a new user in the system.

        Args:
            user_obj (dict): A dictionary containing the user information.

        Returns:
            dict: A dictionary representing the response from the server.

        Raises:
            HTTPError: If the HTTP request to create the user fails.
        """
        response = await self.http_client.post(
            self.folio_client.okapi_url + "/users",
            headers=self.folio_client.okapi_headers,
            json=user_obj,
        )
        response.raise_for_status()
        async with self.lock:
            self.logs["created"] += 1
        return response.json()

    async def create_or_update_user(self, user_obj, existing_user):
        """
        Creates or updates a user based on the given user object and existing user.

        Args:
            user_obj (dict): The user object containing the user details.
            existing_user (dict): The existing user object to be updated, if available.
            logs (dict): A dictionary to keep track of the number of updates and failures.

        Returns:
            dict: The updated or created user object, or an empty dictionary if the operation fails.
        """
        if existing_user:
            existing_user, update_user = await self.update_existing_user(
                user_obj, existing_user
            )
            try:
                update_user.raise_for_status()
                self.logs["updated"] += 1
                return existing_user
            except Exception as e:
                print(
                    f"User update failed: {str(getattr(getattr(e, 'response', str(e)), 'text', str(e)))}"
                )
                await self.logfile.write(
                    f"User update failed: {str(getattr(getattr(e, 'response', str(e)), 'text', str(e)))}\n"
                )
                await self.errorfile.write(
                    json.dumps(existing_user, ensure_ascii=False) + "\n"
                )
                self.logs["failed"] += 1
                return {}
        else:
            try:
                new_user = await self.create_new_user(user_obj)
                return new_user
            except Exception as e:
                print(
                    f"User creation failed: {str(getattr(getattr(e, 'response', str(e)), 'text', str(e)))}"
                )
                await self.logfile.write(
                    f"User creation failed: {str(getattr(getattr(e, 'response', str(e)), 'text', str(e)))}\n"
                )
                await self.errorfile.write(
                    json.dumps(user_obj, ensure_ascii=False) + "\n"
                )
                self.logs["failed"] += 1
                return {}

    async def process_user_obj(self, user: str):
        """
        Process a user object.

        Args:
            user (str): The user data to be processed, as a json string.

        Returns:
            dict: The processed user object.

        """
        user_obj = json.loads(user)
        user_obj["type"] = user_obj.get("type", "patron")
        if "personal" in user_obj:
            current_pref_contact = user_obj["personal"].get(
                "preferredContactTypeId", ""
            )
            user_obj["personal"]["preferredContactTypeId"] = (
                current_pref_contact
                if current_pref_contact in ["001", "002", "003"]
                else "002"
            )
        return user_obj

    async def process_existing_user(self, user_obj):
        """
        Process an existing user.

        Args:
            user_obj (dict): The user object to process.

        Returns:
            tuple: A tuple containing the request preference object (rp_obj),
                   the existing user object, the existing request preference object (existing_rp),
                   and the existing PU object (existing_pu).
        """
        rp_obj = user_obj.pop("requestPreference", {})
        existing_user = await self.get_existing_user(user_obj)
        if existing_user:
            existing_rp = await self.get_existing_rp(user_obj, existing_user)
            existing_pu = await self.get_existing_pu(user_obj, existing_user)
        else:
            existing_rp = {}
            existing_pu = {}
        return rp_obj, existing_user, existing_rp, existing_pu

    async def create_or_update_rp(self, rp_obj, existing_rp, new_user_obj):
        """
        Creates or updates a requet preference object based on the given parameters.

        Args:
            rp_obj (object): A new requet preference object.
            existing_rp (object): The existing resource provider object, if it exists.
            new_user_obj (object): The new user object.

        Returns:
            None
        """
        if existing_rp:
            # print(existing_rp)
            await self.update_existing_rp(rp_obj, existing_rp)
        else:
            # print(new_user_obj)
            await self.create_new_rp(new_user_obj)

    async def create_new_rp(self, new_user_obj):
        """
        Creates a new request preference for a user.

        Args:
            new_user_obj (dict): The user object containing the user's ID.

        Raises:
            HTTPError: If there is an error in the HTTP request.

        Returns:
            None
        """
        rp_obj = {"holdShelf": True, "delivery": False}
        rp_obj["userId"] = new_user_obj["id"]
        # print(rp_obj)
        response = await self.http_client.post(
            self.folio_client.okapi_url
            + "/request-preference-storage/request-preference",
            headers=self.folio_client.okapi_headers,
            json=rp_obj,
        )
        response.raise_for_status()

    async def update_existing_rp(self, rp_obj, existing_rp):
        """
        Updates an existing request preference with the provided request preference object.

        Args:
            rp_obj (dict): The request preference object containing the updated values.
            existing_rp (dict): The existing request preference object to be updated.

        Raises:
            HTTPError: If the PUT request to update the request preference fails.

        Returns:
            None
        """
        existing_rp.update(rp_obj)
        # print(existing_rp)
        response = await self.http_client.put(
            self.folio_client.okapi_url
            + f"/request-preference-storage/request-preference/{existing_rp['id']}",
            headers=self.folio_client.okapi_headers,
            json=existing_rp,
        )
        response.raise_for_status()

    async def create_perms_user(self, new_user_obj):
        """
        Creates a permissions user object for the given new user.

        Args:
            new_user_obj (dict): A dictionary containing the details of the new user.

        Raises:
            HTTPError: If there is an error while making the HTTP request.

        Returns:
            None
        """
        perms_user_obj = {"userId": new_user_obj["id"], "permissions": []}
        response = await self.http_client.post(
            self.folio_client.okapi_url + "/perms/users",
            headers=self.folio_client.okapi_headers,
            json=perms_user_obj,
        )
        response.raise_for_status()

    async def process_line(
        self,
        user: str,
        patron_group_map: dict,
        address_type_map: dict,
    ):
        """
        Process a single line of user data.

        Args:
            user (str): The user data to be processed.
            logs (dict): A dictionary to store logs.
            patron_group_map (dict): A dictionary mapping patron groups.
            address_type_map (dict): A dictionary mapping address types.

        Returns:
            None

        Raises:
            Any exceptions that occur during the processing.

        """
        async with self.limit_simultaneous_requests:
            user_obj = await self.process_user_obj(user)
            rp_obj, existing_user, existing_rp, existing_pu = (
                await self.process_existing_user(user_obj)
            )
            await self.map_address_types(user_obj, address_type_map)
            await self.map_patron_groups(user_obj, patron_group_map)
            new_user_obj = await self.create_or_update_user(user_obj, existing_user)
            if new_user_obj:
                try:
                    if existing_rp or rp_obj:
                        await self.create_or_update_rp(
                            rp_obj, existing_rp, new_user_obj
                        )
                    else:
                        print(
                            f"Creating default request preference object for {new_user_obj['id']}"
                        )
                        await self.logfile.write(
                            f"Creating default request preference object for {new_user_obj['id']}\n"
                        )
                        await self.create_new_rp(new_user_obj)
                except Exception as ee:
                    rp_error_message = (
                        f"Error creating or updating request preferences for "
                        f"{new_user_obj['id']}: "
                        f"{str(getattr(getattr(ee, 'response', ee), 'text', str(ee)))}"
                    )
                    print(rp_error_message)
                    await self.logfile.write(rp_error_message + "\n")
                if not existing_pu:
                    try:
                        await self.create_perms_user(new_user_obj)
                    except Exception as ee:
                        pu_error_message = (
                            f"Error creating permissionUser object for user: {new_user_obj['id']}: "
                            f"{str(getattr(getattr(ee, 'response', str(ee)), 'text', str(ee)))}"
                        )
                        print(pu_error_message)
                        await self.logfile.write(pu_error_message + "\n")

    async def process_file(self):
        patron_group_map = {
            x["group"]: x["id"]
            for x in self.folio_client.folio_get_all("/groups", "usergroups")
        }
        address_type_map = {
            x["addressType"]: x["id"]
            for x in self.folio_client.folio_get_all("/addresstypes", "addressTypes")
        }
        with open(self.user_file_path, "r", encoding="utf-8") as openfile:
            tasks = []
            for user in openfile:
                tasks.append(
                    self.process_line(
                        user,
                        patron_group_map,
                        address_type_map,
                    )
                )
                if len(tasks) == self.batch_size:
                    start = time.time()
                    await asyncio.gather(*tasks)
                    duration = time.time() - start
                    async with self.lock:
                        message = (
                            f"{datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')}: "
                            f"Batch of {self.batch_size} users processed in {duration:.2f} seconds. - "
                            f"Users created: {self.logs['created']} - Users updated: {self.logs['updated']} - "
                            f"Users failed: {self.logs['failed']}"
                        )
                        print(message)
                        await self.logfile.write(message + "\n")
                    tasks = []
            if tasks:
                await asyncio.gather(*tasks)
                async with self.lock:
                    message = (
                        f"{datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')}: "
                        f"Batch of {self.batch_size} users processed in {duration:.2f} seconds. - "
                        f"Users created: {self.logs['created']} - Users updated: {self.logs['updated']} - "
                        f"Users failed: {self.logs['failed']}"
                    )
                    print(message)
                    await self.logfile.write(message + "\n")


async def main():
    """
    Entry point of the user import script.

    Parses command line arguments, initializes necessary objects, and starts the import process.

    Args:
        --tenant_id (str): The tenant id.
        --library_name (str): The name of the library.
        --username (str): The FOLIO username.
        --okapi_url (str): The Okapi URL.
        --user_file_path (str): The path to the user file.
        --limit_async_requests (int): Limit how many http requests can be made at once. Default is 10.
        --batch_size (int): How many user records to process before logging statistics. Default is 250.
        --folio_password (str): The FOLIO password.

    Raises:
        Exception: If an unknown error occurs during the import process.

    Returns:
        None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", help="The tenant id")
    parser.add_argument("--library_name", help="The name of the library")
    parser.add_argument("--username", help="The FOLIO username")
    parser.add_argument("--okapi_url", help="The Okapi URL")
    parser.add_argument("--user_file_path", help="The path to the user file")
    parser.add_argument(
        "--limit_async_requests",
        help="Limit how many http requests can be made at once",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--batch_size",
        help="How many user records to process before logging statistics",
        type=int,
        default=250,
    )
    parser.add_argument("--folio_password", help="The FOLIO password")
    args = parser.parse_args()

    library_name = args.library_name

    # Semaphore to limit the number of async HTTP requests active at any given time
    limit_async_requests = asyncio.Semaphore(args.limit_async_requests)
    batch_size = args.batch_size

    folio_client = folioclient.FolioClient(
        args.okapi_url,
        args.tenant_id,
        args.username,
        args.folio_password
        or os.environ.get("FOLIO_PASS", "")
        or getpass.getpass("Enter your FOLIO password: "),
    )
    user_file_path = Path(args.user_file_path)
    log_file_path = (
        user_file_path.parent.parent
        / "reports"
        / f"log_user_import_{datetime.datetime.now(utc).strftime('%Y%m%d_%H%M%S')}.log"
    )
    error_file_path = (
        user_file_path.parent
        / f"failed_user_import_{datetime.datetime.now(utc).strftime('%Y%m%d_%H%M%S')}.txt"
    )
    async with aiofiles.open(
        log_file_path,
        "w",
    ) as logfile, aiofiles.open(
        error_file_path, "w"
    ) as errorfile, httpx.AsyncClient(timeout=None) as http_client:
        try:
            importer = UserImporter(
                folio_client,
                library_name,
                user_file_path,
                batch_size,
                limit_async_requests,
                logfile,
                errorfile,
                http_client,
            )
            await importer.do_import()
        except Exception as e:
            print(f"An unknown error occurred: {e}")
            await logfile.write(f"An error occurred {e}\n")
            raise e


def sync_main():
    """
    Synchronous version of the main function.

    This function is used to run the main function in a synchronous context.
    """
    asyncio.run(main())


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
