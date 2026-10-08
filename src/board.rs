//! Channels and tallies. A passing vote writes moderation. It does not admit.

use std::collections::BTreeSet;

use crate::error::{err, Result};
use crate::state::{PostView, State};

pub const GENERAL: &str = "general";

pub struct Passing {
    pub target: String,
    pub channel: String,
    pub action: String,
    pub weight: u64,
}

pub fn channel_ok(name: &str) -> bool {
    let n = name.len();
    (1..=32).contains(&n)
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

pub fn ensure_channel(name: &str) -> Result<()> {
    if channel_ok(name) {
        Ok(())
    } else {
        Err(err("bad channel"))
    }
}

/// Mute, demote, move, flag, pin. Anything else, including kill and budget, is refused.
pub fn ensure_choice(choice: &str, channel: &str) -> Result<()> {
    ensure_channel(channel)?;
    if !matches!(choice, "mute" | "demote" | "move" | "flag" | "pin") {
        return Err(err(format!("a vote cannot {choice}")));
    }
    if choice == "move" && channel == GENERAL {
        return Err(err("general stays open"));
    }
    Ok(())
}

pub fn weight_for(role: &str, human_weight: u64, demoted: bool) -> u64 {
    if demoted {
        return 0;
    }
    match role {
        "human" => human_weight,
        _ => 1,
    }
}

pub fn muted(state: &State, id: &str) -> bool {
    state
        .moderation
        .iter()
        .any(|m| m.target == id && m.action == "mute")
}

pub fn demoted(state: &State, id: &str) -> bool {
    state
        .moderation
        .iter()
        .any(|m| m.target == id && m.action == "demote")
}

pub fn moved(state: &State, id: &str, channel: &str) -> bool {
    state
        .moderation
        .iter()
        .any(|m| m.target == id && m.action == "move" && m.channel == channel)
}

/// `general`, plus a channel named for each tag, unless a move took it.
pub fn ensure_post(tags: &[String], state: &State, author: &str, channel: &str) -> Result<()> {
    ensure_channel(channel)?;
    if channel == GENERAL {
        return Ok(());
    }
    if tags.iter().any(|tag| tag == channel) && !moved(state, author, channel) {
        return Ok(());
    }
    Err(err(format!("not on {channel}")))
}

pub fn show_post(state: &State, reader: &str, tags: &[String], post: &PostView) -> bool {
    if muted(state, &post.author) {
        return false;
    }
    let home = post.channel == GENERAL
        || (tags.iter().any(|tag| tag == &post.channel) && !moved(state, reader, &post.channel));
    let mentioned = post
        .mentions
        .iter()
        .any(|mention| mention == "all" || mention == reader);
    home || mentioned
}

pub fn operator_sees(state: &State, post: &PostView) -> bool {
    !muted(state, &post.author)
}

/// Winning choice weighs at least `human_weight` and more than every other choice.
pub fn passing(state: &State, human_weight: u64) -> Vec<Passing> {
    let mut pairs = BTreeSet::new();
    for (_, target, channel) in state.votes.keys() {
        pairs.insert((target.clone(), channel.clone()));
    }
    let mut out = Vec::new();
    for (target, channel) in pairs {
        let Some((action, weight)) = winner(state, &target, &channel, human_weight) else {
            continue;
        };
        if state
            .moderation
            .iter()
            .any(|m| m.target == target && m.action == action && m.channel == channel)
        {
            continue;
        }
        out.push(Passing {
            target,
            channel,
            action,
            weight,
        });
    }
    out
}

fn winner(state: &State, target: &str, channel: &str, human_weight: u64) -> Option<(String, u64)> {
    let mut scores: Vec<(String, u64)> = Vec::new();
    for ((_, vote_target, vote_channel), vote) in &state.votes {
        if vote_target != target || vote_channel != channel || vote.weight == 0 {
            continue;
        }
        if let Some(slot) = scores.iter_mut().find(|(choice, _)| choice == &vote.choice) {
            slot.1 += vote.weight;
        } else {
            scores.push((vote.choice.clone(), vote.weight));
        }
    }
    scores.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let (choice, weight) = scores.first()?;
    let next = scores.get(1).map(|(_, weight)| *weight).unwrap_or(0);
    if *weight >= human_weight && *weight > next {
        Some((choice.clone(), *weight))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Record;
    use crate::state::{PostView, State};

    fn fresh() -> State {
        State::new(&crate::config::preset("box"))
    }

    fn vote(voter: &str, target: &str, choice: &str, weight: u64) -> Record {
        Record::Vote {
            id: format!("{voter}-{choice}"),
            voter: voter.into(),
            role: "worker".into(),
            target: target.into(),
            channel: GENERAL.into(),
            choice: choice.into(),
            weight,
            ts: 1,
        }
    }

    #[test]
    fn humans_outweigh_workers() {
        let mut state = fresh();
        for voter in ["a", "b", "c"] {
            state.apply(&vote(voter, "t", "mute", 1));
        }
        assert!(passing(&state, 4).is_empty());
        state.apply(&vote("you", "t", "mute", 4));
        let due = passing(&state, 4);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].action, "mute");
        assert_eq!(due[0].weight, 7);
        state.apply(&Record::Moderation {
            id: "m".into(),
            target: "t".into(),
            action: "mute".into(),
            channel: GENERAL.into(),
            weight: 7,
            ts: 2,
        });
        assert!(passing(&state, 4).is_empty());
    }

    #[test]
    fn a_tie_and_a_replaced_vote_do_not_pass() {
        let mut state = fresh();
        state.apply(&vote("h", "t", "mute", 4));
        state.apply(&vote("w", "t", "pin", 4));
        assert!(passing(&state, 4).is_empty());

        let mut replaced = fresh();
        replaced.apply(&vote("a", "t", "mute", 4));
        replaced.apply(&vote("a", "t", "pin", 1));
        assert!(passing(&replaced, 4).is_empty());
        assert_eq!(replaced.votes.len(), 1);
        assert_eq!(
            replaced
                .votes
                .values()
                .next()
                .map(|vote| vote.choice.as_str()),
            Some("pin")
        );
    }

    #[test]
    fn channels_mentions_and_refused_choices() {
        let mut state = fresh();
        assert!(ensure_choice("kill", GENERAL).is_err());
        assert!(ensure_choice("budget", GENERAL).is_err());
        assert!(ensure_choice("admit", "code").is_err());
        assert!(ensure_choice("move", GENERAL).is_err());
        assert!(ensure_choice("mute", GENERAL).is_ok());
        assert!(ensure_choice("move", "code").is_ok());
        assert!(ensure_post(&["code".into()], &state, "w", "code").is_ok());
        assert!(ensure_post(&["math".into()], &state, "m", "code").is_err());
        assert!(ensure_post(&["math".into()], &state, "m", GENERAL).is_ok());
        assert!(ensure_post(&["math".into()], &state, "m", "Nope").is_err());

        let secret = PostView {
            id: "p".into(),
            author: "w".into(),
            role: "worker".into(),
            text: "secret".into(),
            weight: 1,
            channel: "code".into(),
            mentions: Vec::new(),
            ts: 1,
        };
        assert!(!show_post(&state, "m", &["math".into()], &secret));
        let mut mentioned = secret.clone();
        mentioned.mentions = vec!["m".into()];
        assert!(show_post(&state, "m", &["math".into()], &mentioned));

        state.apply(&Record::Moderation {
            id: "mv".into(),
            target: "w".into(),
            action: "move".into(),
            channel: "code".into(),
            weight: 4,
            ts: 2,
        });
        assert!(ensure_post(&["code".into()], &state, "w", "code").is_err());
        assert!(ensure_post(&["code".into()], &state, "w", GENERAL).is_ok());
        state.apply(&Record::Moderation {
            id: "mu".into(),
            target: "w".into(),
            action: "mute".into(),
            channel: GENERAL.into(),
            weight: 4,
            ts: 3,
        });
        assert!(!show_post(&state, "m", &["math".into()], &mentioned));
        assert!(!operator_sees(&state, &mentioned));
        assert_eq!(weight_for("human", 4, true), 0);
        assert_eq!(weight_for("worker", 4, false), 1);
        assert_eq!(weight_for("operator", 4, false), 1);
    }
}
