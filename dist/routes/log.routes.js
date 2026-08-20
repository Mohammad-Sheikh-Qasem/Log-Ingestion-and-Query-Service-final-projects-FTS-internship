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
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const log_controller_js_1 = require("../controllers/log.controller.js");
const query_controller_js_1 = require("../controllers/query.controller.js");
const aggregate_controller_js_1 = require("../controllers/aggregate.controller.js");
const retention_service_1 = require("../services/retention.service");
const router = (0, express_1.Router)();
router.post('/logs', log_controller_js_1.ingestLogsHandler);
router.get('/logs', query_controller_js_1.queryLogsHandler);
router.get('/logs/aggregate', aggregate_controller_js_1.aggregateLogsHandler);
router.post('/admin/retention/run', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const deletedCount = yield (0, retention_service_1.cleanupOldLogs)();
    res.status(200).json({ status: 'success', deletedCount });
}));
exports.default = router;
