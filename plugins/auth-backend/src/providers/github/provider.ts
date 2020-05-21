/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import passport from 'passport';
import express from 'express';
import { Strategy as GithubStrategy } from 'passport-github2';
import {
  AuthProvider,
  AuthProviderRouteHandlers,
  AuthProviderConfig,
} from './../types';
import { postMessageResponse } from './../utils';

export class GithubAuthProvider
  implements AuthProvider, AuthProviderRouteHandlers {
  private readonly providerConfig: AuthProviderConfig;
  constructor(providerConfig: AuthProviderConfig) {
    this.providerConfig = providerConfig;
  }

  start(
    req: express.Request,
    res: express.Response,
    next: express.NextFunction,
  ) {
    const scopes = req.query.scopes?.toString().split(',');
    return passport.authenticate('github', { scope: scopes })(req, res, next);
  }

  frameHandler(
    req: express.Request,
    res: express.Response,
    next: express.NextFunction,
  ) {
    return passport.authenticate('github', (_, user) => {
      postMessageResponse(res, {
        type: 'auth-result',
        payload: user,
      });
    })(req, res, next);
  }

  logout(_req: express.Request, res: express.Response) {
    return new Promise(resolve => {
      res.send('logout!');
      resolve();
    });
  }

  strategy(): passport.Strategy {
    return new GithubStrategy(
      { ...this.providerConfig.options, passReqToCallback: true },
      (
        _req: any,
        accessToken: any,
        refreshToken: any,
        profile: any,
        done: any,
      ) => {
        done(undefined, { profile, accessToken, refreshToken });
      },
    );
  }
}
