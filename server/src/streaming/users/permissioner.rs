use crate::streaming::users::permissions::{GlobalPermissions, StreamPermissions};
use crate::streaming::users::user::User;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Permissioner {
    pub(super) enabled: bool,
    pub(super) users_permissions: HashMap<u32, GlobalPermissions>,
    pub(super) users_streams_permissions: HashMap<(u32, u32), StreamPermissions>,
    pub(super) users_that_can_poll_messages_from_all_streams: HashSet<u32>,
    pub(super) users_that_can_send_messages_to_all_streams: HashSet<u32>,
    pub(super) users_that_can_poll_messages_from_specific_streams: HashSet<(u32, u32)>,
    pub(super) users_that_can_send_messages_to_specific_streams: HashSet<(u32, u32)>,
}

impl Permissioner {
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn init(&mut self, users: Vec<User>) {
        for user in users {
            if user.permissions.is_none() {
                continue;
            }

            let permissions = user.permissions.unwrap();
            if permissions.global.poll_messages {
                self.users_that_can_poll_messages_from_all_streams
                    .insert(user.id);
            }

            if permissions.global.send_messages {
                self.users_that_can_send_messages_to_all_streams
                    .insert(user.id);
            }

            self.users_permissions.insert(user.id, permissions.global);
            if permissions.streams.is_none() {
                continue;
            }

            let streams = permissions.streams.unwrap();
            for (stream_id, stream) in streams {
                if stream.poll_messages {
                    self.users_that_can_poll_messages_from_specific_streams
                        .insert((user.id, stream_id));
                }

                if stream.send_messages {
                    self.users_that_can_send_messages_to_specific_streams
                        .insert((user.id, stream_id));
                }

                self.users_streams_permissions
                    .insert((user.id, stream_id), stream);
            }
        }
    }
}