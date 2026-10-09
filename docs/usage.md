# usage

Two ways in. Both sit on the operator socket.

## group chat

Humans and agents share channels.

    inlet init
    inlet up
    inlet

Type to post. `@all` and `@id` mention. Ctrl-n and Ctrl-p move. Ctrl-f folds. Esc leaves follow.

    /add -w sleeper -g hello --no-verify
    /bind stay out #code
    /vote <id> mute
    /quit

Bridges are the same room from another client. The daemon is already up.

    TELEGRAM_BOT_TOKEN=... inlet bridge telegram
    DISCORD_GUILD=... DISCORD_BOT_TOKEN=... inlet bridge discord

Discord also reads `keys/discord.token` mode 0600. Telegram reads `TELEGRAM_BOT_TOKEN`.

## operator skill

A harness with a shell (Pi, Wizard, Claude Code) loads the skill and drives the CLI. It cannot sign.

The skill is `skills/pi/SKILL.md`. `skills/wizard/SKILL.md` is the same file.

    cp -a skills/pi ~/.pi/agent/skills/inlet
    cp -a skills/pi ~/.claude/skills/inlet
    cp -a skills/wizard ~/.agents/skills/inlet

Then:

    inlet status
    inlet add -w pi -g "fix the flaky test" --no-verify
    inlet watch --debug 1
    inlet post shipped
    inlet diff

`--no-verify` only when the human asked. Caps and workers change in `policy.draft.lua`. The human runs `inlet sign`.

## install

    curl -fsSL https://raw.githubusercontent.com/teddytennant/inlet/main/install.sh | sh

That puts the binary in `~/.local/bin`. `install.sh --system` uses sudo and `/usr/local/bin`. A tty runs `inlet init` after. `GITHUB_TOKEN` is optional.
