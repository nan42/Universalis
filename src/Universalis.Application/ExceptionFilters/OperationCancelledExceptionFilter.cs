﻿using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;
using System;
using Microsoft.AspNetCore.Http;

namespace Universalis.Application.ExceptionFilters;

public class OperationCancelledExceptionFilter : IExceptionFilter
{
    private readonly ILogger _logger;

    public OperationCancelledExceptionFilter(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<OperationCancelledExceptionFilter>();
    }

    public void OnException(ExceptionContext context)
    {
        if (context.Exception is not OperationCanceledException) return;
        _logger.LogWarning("Request was cancelled");
        context.Result = new StatusCodeResult(504);
        context.ExceptionHandled = true;
        context.HttpContext.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
    }
}