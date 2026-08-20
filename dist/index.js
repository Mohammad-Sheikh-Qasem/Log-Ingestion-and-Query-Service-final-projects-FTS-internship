"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const dotenv_1 = __importDefault(require("dotenv"));
const index_js_1 = require("./db/index.js");
const migrate_js_1 = require("./db/migrate.js");
const log_routes_js_1 = __importDefault(require("./routes/log.routes.js"));
const index_js_2 = require("./services/index.js");
dotenv_1.default.config();
const app = (0, express_1.default)();
const PORT = process.env.PORT || 8080;
app.use(express_1.default.json());
app.use((err, req, res, next) => {
    if (err instanceof SyntaxError && 'body' in err) {
        return res.status(400).json({ error: 'Malformed JSON in request body' });
    }
    next(err);
});
app.get('/health', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        yield index_js_1.pool.query(' SELECT 1');
        res.status(200).json({ status: 'healthy', database: 'connected' });
    }
    catch (error) {
        res.status(500).json({ status: 'unhealthy', database: 'disconnected' });
    }
}));
app.use(log_routes_js_1.default);
// app.listen(PORT, () => {
//     console.log(`Server is running on port ${PORT}`);
// });
function startServer() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            yield (0, migrate_js_1.runMigration)();
            (0, index_js_2.startRetentionScheduler)();
            app.listen(PORT, () => {
                console.log(`Server is running on port ${PORT}`);
            });
        }
        catch (error) {
            console.error('Failed to start server', error);
            process.exit(1);
        }
    });
}
startServer();
