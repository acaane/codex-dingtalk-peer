use std::io;

use serde::{Deserialize, Serialize};

use crate::room::{self, Member, Secret};

pub type Id = [u8; 16];
const LEASE_SECONDS: u64 = 150;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Failure {
    Session,
    Busy,
    Model,
    Timeout,
    Reply,
    Interrupted,
}

impl Failure {
    pub fn label(self) -> &'static str {
        match self {
            Self::Session => "请检查本机 Codex 配置或重新绑定会话",
            Self::Busy => "请等本机会话结束当前回合",
            Self::Model => "请检查本机 Codex 的登录或模型连接",
            Self::Timeout => "等待本机 Codex 回报超时",
            Self::Reply => "本机 Codex 回报的校验码不符",
            Self::Interrupted => "验证中途断开",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Unbound,
    Waiting,
    Running,
    Passed,
    Failed(Failure),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Request {
    pub binding: Option<Secret>,
    pub attempt: Id,
    pub retry: bool,
    pub result: Option<Report>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Report {
    pub challenge: Id,
    pub answer: Result<Id, Failure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub host: Status,
    pub guest: Status,
    pub challenge: Option<Id>,
}

impl Update {
    pub fn status(self, member: Member) -> Status {
        match member {
            Member::Host => self.host,
            Member::Guest => self.guest,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
enum Phase {
    #[default]
    Waiting,
    Running {
        attempt: Id,
        challenge: Id,
        deadline: u64,
    },
    Passed,
    Failed {
        attempt: Id,
        reason: Failure,
    },
}

#[derive(Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
struct Participant {
    binding: Option<Secret>,
    phase: Phase,
}

impl Participant {
    fn status(self) -> Status {
        if self.binding.is_none() {
            return Status::Unbound;
        }
        match self.phase {
            Phase::Waiting => Status::Waiting,
            Phase::Running { .. } => Status::Running,
            Phase::Passed => Status::Passed,
            Phase::Failed { reason, .. } => Status::Failed(reason),
        }
    }
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct State {
    host: Participant,
    guest: Participant,
}

impl State {
    pub fn verified_bindings(&self) -> Option<[Secret; 2]> {
        if self.host.phase == Phase::Passed && self.guest.phase == Phase::Passed {
            return Some([self.host.binding?, self.guest.binding?]);
        }
        None
    }

    pub fn apply(&mut self, member: Member, request: Request, now: u64) -> io::Result<Update> {
        for participant in [&mut self.host, &mut self.guest] {
            if let Phase::Running {
                attempt, deadline, ..
            } = participant.phase
                && now >= deadline
            {
                participant.phase = Phase::Failed {
                    attempt,
                    reason: Failure::Interrupted,
                };
            }
        }
        let (local, peer) = match member {
            Member::Host => (&mut self.host, &self.guest),
            Member::Guest => (&mut self.guest, &self.host),
        };
        if local.binding != request.binding {
            local.binding = request.binding;
            local.phase = Phase::Waiting;
        }
        if let Phase::Running {
            attempt, challenge, ..
        } = local.phase
            && attempt == request.attempt
            && let Some(report) = request.result
            && report.challenge == challenge
        {
            local.phase = match report.answer {
                Ok(answer) if answer == challenge => Phase::Passed,
                reply => Phase::Failed {
                    attempt,
                    reason: reply.err().unwrap_or(Failure::Reply),
                },
            };
        }
        let retry = matches!(local.phase, Phase::Failed { attempt, .. } if request.retry && attempt != request.attempt);
        if local.binding.is_some()
            && peer.binding.is_some()
            && (local.phase == Phase::Waiting || retry)
        {
            local.phase = Phase::Running {
                attempt: request.attempt,
                challenge: room::random()?,
                deadline: now + LEASE_SECONDS,
            };
        }
        // Polls can resend a claim, but only its original attempt can receive the challenge.
        let challenge = match local.phase {
            Phase::Running {
                attempt, challenge, ..
            } if attempt == request.attempt => Some(challenge),
            _ => None,
        };
        Ok(Update {
            host: self.host.status(),
            guest: self.guest.status(),
            challenge,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_challenge_belongs_to_one_attempt_and_wrong_answers_need_an_explicit_retry() {
        let mut state = State::default();
        let host = Request {
            binding: Some([1; 32]),
            attempt: [1; 16],
            retry: false,
            result: None,
        };
        let guest = Request {
            binding: Some([2; 32]),
            attempt: [2; 16],
            ..host
        };
        assert!(
            state
                .apply(Member::Host, host, 0)
                .unwrap()
                .challenge
                .is_none()
        );
        let challenge = state
            .apply(Member::Guest, guest, 1)
            .unwrap()
            .challenge
            .unwrap();
        let unrelated = Request {
            attempt: [3; 16],
            result: Some(Report {
                challenge,
                answer: Ok(challenge),
            }),
            ..guest
        };
        let update = state.apply(Member::Guest, unrelated, 2).unwrap();
        assert_eq!(update.guest, Status::Running);
        assert_eq!(update.challenge, None);
        let wrong = Request {
            result: Some(Report {
                challenge,
                answer: Ok([3; 16]),
            }),
            ..guest
        };
        assert_eq!(
            state.apply(Member::Guest, wrong, 3).unwrap().guest,
            Status::Failed(Failure::Reply)
        );
        assert_eq!(
            state.apply(Member::Guest, guest, 4).unwrap().challenge,
            None
        );
        let retry = Request {
            attempt: [4; 16],
            retry: true,
            ..guest
        };
        let challenge = state
            .apply(Member::Guest, retry, 5)
            .unwrap()
            .challenge
            .unwrap();
        let result = Request {
            result: Some(Report {
                challenge,
                answer: Ok(challenge),
            }),
            ..retry
        };
        let update = state.apply(Member::Guest, result, 6).unwrap();
        assert_eq!(update.guest, Status::Passed);
        assert_eq!(update.host, Status::Waiting);
        assert_eq!(update.challenge, None);
    }

    #[test]
    fn interrupted_validation_expires_and_a_late_report_cannot_pass() {
        let mut state = State::default();
        let host = Request {
            binding: Some([1; 32]),
            attempt: [1; 16],
            retry: false,
            result: None,
        };
        let guest = Request {
            binding: Some([2; 32]),
            attempt: [2; 16],
            ..host
        };
        state.apply(Member::Host, host, 0).unwrap();
        let challenge = state
            .apply(Member::Guest, guest, 1)
            .unwrap()
            .challenge
            .unwrap();
        let late = Request {
            result: Some(Report {
                challenge,
                answer: Ok(challenge),
            }),
            ..guest
        };
        assert_eq!(
            state
                .apply(Member::Guest, late, LEASE_SECONDS + 1)
                .unwrap()
                .guest,
            Status::Failed(Failure::Interrupted)
        );
    }
}
