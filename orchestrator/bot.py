"""
YouTube Pipeline Orchestrator Bot
Runs 24/7 on Railway.app — controls everything from Telegram
"""

import os, json, asyncio, logging, time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ConversationHandler, filters, ContextTypes
)
from pipeline_stages import PipelineManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Conversation states ──────────────────────────────────────────────────────
TOPIC, DESCRIPTION, PUBLISH_TIME, CONFIRM = range(4)

ALLOWED_USER_ID = int(os.environ["ALLOWED_USER_ID"])

def auth(func):
    """Decorator: only you can use this bot."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ALLOWED_USER_ID:
            await update.message.reply_text("⛔ Unauthorised.")
            return
        return await func(update, ctx)
    return wrapper

# ── /start ───────────────────────────────────────────────────────────────────
@auth
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🎬 New Video Project", callback_data="new_project")],
        [InlineKeyboardButton("📊 Check Status",      callback_data="status")],
        [InlineKeyboardButton("📁 List Projects",     callback_data="list_projects")],
    ]
    await update.message.reply_text(
        "👋 *YouTube Pipeline Bot*\n\nWhat would you like to do?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )

# ── /status ──────────────────────────────────────────────────────────────────
@auth
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pm = PipelineManager()
    status_text = pm.get_status_report()
    await update.message.reply_text(status_text, parse_mode="Markdown")

# ── /resume ──────────────────────────────────────────────────────────────────
@auth
async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pm = PipelineManager()
    msg = pm.resume_paused_pipeline()
    await update.message.reply_text(msg, parse_mode="Markdown")

# ── Callback buttons ─────────────────────────────────────────────────────────
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "new_project":
        await query.message.reply_text(
            "📝 *Step 1/4* — What is the *topic* of your video?\n\n"
            "Example: `Bhagat Singh Documentary in Hindi`",
            parse_mode="Markdown"
        )
        return TOPIC

    elif query.data == "status":
        pm = PipelineManager()
        await query.message.reply_text(pm.get_status_report(), parse_mode="Markdown")

    elif query.data == "list_projects":
        pm = PipelineManager()
        await query.message.reply_text(pm.list_projects(), parse_mode="Markdown")

# ── Conversation: collect project details ────────────────────────────────────
@auth
async def get_topic(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["topic"] = update.message.text
    await update.message.reply_text(
        "📝 *Step 2/4* — Give a detailed description:\n\n"
        "Example: `Story of Bhagat Singh, focusing on his revolutionary activities "
        "and sacrifice for Indian independence. Target audience: Hindi speakers aged 18-35.`",
        parse_mode="Markdown"
    )
    return DESCRIPTION

@auth
async def get_description(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["description"] = update.message.text
    await update.message.reply_text(
        "📝 *Step 3/4* — When should it publish?\n\n"
        "Format: `YYYY-MM-DD HH:MM`\n"
        "Example: `2026-05-25 17:00`\n\n"
        "Or send `asap` to publish immediately after processing.",
        parse_mode="Markdown"
    )
    return PUBLISH_TIME

@auth
async def get_publish_time(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["publish_time"] = update.message.text

    summary = (
        f"✅ *Project Summary*\n\n"
        f"🎬 *Topic:* {ctx.user_data['topic']}\n"
        f"📄 *Description:* {ctx.user_data['description'][:200]}...\n"
        f"🕐 *Publish:* {ctx.user_data['publish_time']}\n\n"
        f"Launch the pipeline?"
    )
    kb = [
        [InlineKeyboardButton("🚀 Launch Pipeline", callback_data="launch")],
        [InlineKeyboardButton("❌ Cancel",           callback_data="cancel")],
    ]
    await update.message.reply_text(
        summary, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return CONFIRM

@auth
async def confirm_launch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "cancel":
        await query.message.reply_text("❌ Cancelled. Send /start to begin again.")
        return ConversationHandler.END

    # Launch the pipeline
    pm = PipelineManager()
    project_id = pm.create_project(
        topic=ctx.user_data["topic"],
        description=ctx.user_data["description"],
        publish_time=ctx.user_data["publish_time"],
        chat_id=query.message.chat_id
    )

    await query.message.reply_text(
        f"🚀 *Pipeline Launched!*\n\n"
        f"Project ID: `{project_id}`\n\n"
        f"I'll send you updates at each stage. "
        f"You can check progress anytime with /status",
        parse_mode="Markdown"
    )

    # Trigger Colab worker asynchronously
    asyncio.create_task(pm.trigger_colab_worker(project_id, query.message.chat_id))

    return ConversationHandler.END

# ── Handle file uploads (manual video assets from user) ──────────────────────
@auth
async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """When user sends a video file back — it's a manual asset for a paused scene."""
    pm = PipelineManager()
    doc = update.message.document or update.message.video

    if not doc:
        return

    file = await ctx.bot.get_file(doc.file_id)
    file_name = getattr(doc, "file_name", f"asset_{int(time.time())}.mp4")

    await update.message.reply_text(f"📥 Receiving `{file_name}`...", parse_mode="Markdown")

    result = await pm.receive_manual_asset(file, file_name)
    await update.message.reply_text(result, parse_mode="Markdown")

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    token = os.environ["TELEGRAM_TOKEN"]
    app = Application.builder().token(token).build()

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^new_project$")],
        states={
            TOPIC:        [MessageHandler(filters.TEXT & ~filters.COMMAND, get_topic)],
            DESCRIPTION:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_description)],
            PUBLISH_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_publish_time)],
            CONFIRM:      [CallbackQueryHandler(confirm_launch, pattern="^(launch|cancel)$")],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
    )

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.Document.ALL | filters.VIDEO, handle_document))

    logger.info("Bot started — polling...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
