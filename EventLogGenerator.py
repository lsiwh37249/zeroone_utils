import random
import uuid
import csv
from datetime import datetime, timedelta
import os


class EventLogGenerator:
    def __init__(self, output_dir="output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def random_user_id(self):
        return f"user_{random.randint(1, 100)}"

    def random_study_id(self):
        return f"study_{random.randint(1, 300)}"

    def generate_timestamp(self, date_str):
        base = datetime.strptime(date_str, "%Y-%m-%d")
        offset = timedelta(minutes=random.randint(0, 1439))
        return (base + offset).isoformat() + "Z"

    def member_login(self, date):
        return {
            "event": "member_login",
            "timestamp": self.generate_timestamp(date),
            "dl_member_id": self.random_user_id(),
            "dl_login_method": random.choice(["kakao", "google", "email"])
        }

    def member_logout(self, date):
        return {
            "event": "member_logout",
            "timestamp": self.generate_timestamp(date),
            "dl_member_id": self.random_user_id()
        }

    def member_join(self, date):
        return {
            "event": "member_join",
            "timestamp": self.generate_timestamp(date),
            "dl_member_id": self.random_user_id()
        }

    def member_delete(self, date):
        return {
            "event": "member_delete",
            "timestamp": self.generate_timestamp(date),
            "dl_member_id": self.random_user_id()
        }

    def study_matching(self, date):
        user1 = self.random_user_id()
        user2 = self.random_user_id()
        while user1 == user2:
            user2 = self.random_user_id()
        return {
            "event": "study_matching",
            "timestamp": self.generate_timestamp(date),
            "dl_member_id": user1,
            "dl_study_id": self.random_study_id(),
            "dl_matched_with": user2,
            "dl_matching_type": random.choice(["AUTO", "MANUAL"])
        }

    def study_cancel(self, date):
        return {
            "event": "study_cancel",
            "timestamp": self.generate_timestamp(date),
            "dl_member_id": self.random_user_id(),
            "dl_study_id": self.random_study_id(),
            "dl_cancel_reason": random.choice(["시간 변경", "일정 충돌", "의사소통 문제"])
        }

    def study_complete(self, date):
        user = self.random_user_id()
        return {
            "event": "study_complete",
            "timestamp": self.generate_timestamp(date),
            "dl_study_id": self.random_study_id(),
            "dl_member_id": user,
            "dl_attendee_id": user
        }

    def generate_events(self, date, count=50):
        generators = [
            self.member_login,
            self.member_logout,
            self.member_join,
            self.member_delete,
            self.study_matching,
            self.study_cancel,
            self.study_complete
        ]
        events = [random.choice(generators)(date) for _ in range(count)]
        return events

    def save_to_csv(self, date, count=50):
        events = self.generate_events(date, count)
        all_keys = set(k for event in events for k in event.keys())
        sorted_keys = sorted(all_keys)
        date_nodash = date.replace("-", "")
        filename = f"{self.output_dir}/event_log_{date_nodash}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted_keys)
            writer.writeheader()
            writer.writerows(events)

        print(f"✅ Saved {len(events)} events to {filename}")

