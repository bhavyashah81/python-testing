class InMemoryDatabase:
    def __init__(self):
        self.db = {}  # Main database: {key: {field: value}}
        self.modifications = {}  # Tracks the number of modifications per key
        self.locks = {}  # Tracks locks on records: {key: {"owner": caller_id, "queue": []}}

    def set_or_inc_by_caller(self, key, field, value, caller_id):
        """
        Inserts or updates a field in the database with respect to the lock.
        """
        if key not in self.db:
            return None  # Key doesn't existt

        # Check locking rules
        if key in self.locks:
            if caller_id is None or self.locks[key]["owner"] != caller_id:
                return None  # Ignore if caller doesn't own the lock or no caller specified

        # Perform the operation
        if field in self.db[key]:
            self.db[key][field] += value
        else:
            self.db[key][field] = value

        # Increment modification count
        if key not in self.modifications:
            self.modifications[key] = 0
        self.modifications[key] += 1

        return self.db[key][field]

    def delete_by_caller(self, key, field, caller_id):
        """
        Deletes a field in the database with respect to the lock.
        """
        if key not in self.db:
            return False  # Key doesn't exist

        # Check locking rules
        if key in self.locks:
            if caller_id is None or self.locks[key]["owner"] != caller_id:
                return False  # Ignore if caller doesn't own the lock or no caller specified

        # Perform the deletion
        if field in self.db[key]:
            del self.db[key][field]

            # If no fields remain, delete the key entirely
            if not self.db[key]:
                del self.db[key]
                del self.modifications[key]

            return True

        return False

    def lock(self, caller_id, key):
        """
        Requests a lock on the record associated with the given key.
        """
        if key not in self.db:
            return "invalid_request"

        if key not in self.locks:
            self.locks[key] = {"owner": caller_id, "queue": []}
            return "acquired"

        if self.locks[key]["owner"] == caller_id:
            return None  # Already locked by this user

        self.locks[key]["queue"].append(caller_id)
        return "wait"

    def unlock(self, key):
        """
        Releases the lock on the record associated with the given key.
        """
        if key not in self.db and key not in self.locks:
            return "invalid_request"

        if key not in self.locks:
            return None  # No lock exists for the key

        if key in self.locks:
            if self.locks[key]["queue"]:
                # Assign the lock to the next user in the queue
                next_caller = self.locks[key]["queue"].pop(0)
                self.locks[key]["owner"] = next_caller
            else:
                # No one is in the queue; remove the lock
                del self.locks[key]

        if key not in self.db:
            return "released"  # Record was deleted during this lock

        return "released"
